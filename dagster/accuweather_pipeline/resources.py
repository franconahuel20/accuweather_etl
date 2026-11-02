"""Resources shared by the assets: AccuWeather API client, MinIO (S3-compatible) storage and the
PostgreSQL warehouse. The request/naming helpers mirror the original AWS Lambda (accuweather.py)."""
import json
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Iterator, NamedTuple, Optional
from zoneinfo import ZoneInfo

import boto3
import psycopg2
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dagster import ConfigurableResource
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Raw API responses for the observations live under this prefix (same layout as the original S3 bucket).
CURRENT_CONDITIONS_PREFIX = "currentconditions/historical/"

# kind_of_request -> (endpoint path, query parameters, object-storage base prefix)
REQUEST_KINDS = {
    "historical_current_conditions": (
        "/currentconditions/v1/{accuweather_id}/historical/24",
        {"language": "en-us", "details": "true"},
        "currentconditions/historical",
    ),
    "forecasts_5_days": (
        "/forecasts/v1/daily/5day/{accuweather_id}",
        {"language": "en-us", "details": "true", "metric": "true"},
        "forecasts",
    ),
}

# Locale-independent month abbreviations for the file names (strftime('%b') depends on the locale).
MONTH_ABBR = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


class City(NamedTuple):
    accuweather_id: str
    name: str
    country_iso_code: str
    country_name: str
    tz: str
    utc: int
    latitude: float
    longitude: float
    folder: str


class AccuWeatherError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


def validate_kind_of_request(kind_of_request: str) -> None:
    if kind_of_request not in REQUEST_KINDS:
        raise ValueError(f"Invalid kind of request '{kind_of_request}'. Valid values: {sorted(REQUEST_KINDS)}.")


def set_dest_prefix(kind_of_request: str, city: City) -> str:
    validate_kind_of_request(kind_of_request)
    return f"{REQUEST_KINDS[kind_of_request][2]}/{city.country_iso_code}/{city.folder}/"


def target_datetime(city: City, now: Optional[datetime] = None) -> datetime:
    """The pipeline runs after local midnight to fetch the previous day, so files are named after yesterday."""
    local_now = (now or datetime.now(tz=ZoneInfo("UTC"))).astimezone(ZoneInfo(city.tz))
    return local_now - timedelta(days=1)


def build_object_key(kind_of_request: str, city: City, target_dt: datetime) -> str:
    file_name = (
        f"{target_dt:%Y}_{MONTH_ABBR[target_dt.month - 1]}_{target_dt:%d}_{city.folder}_{city.country_iso_code}.json"
    )
    return set_dest_prefix(kind_of_request, city) + file_name


def enrich_with_location(data: Any, city: City) -> Any:
    """Adds the location fields to every observation, exactly like the original Lambda."""
    location = {
        "city_accuweather_id": city.accuweather_id,
        "city_name": city.name,
        "country_iso_code": city.country_iso_code,
        "country_name": city.country_name,
        "latitude": city.latitude,
        "longitude": city.longitude,
    }
    records = data if isinstance(data, list) else [data]
    for record in records:
        if isinstance(record, dict):
            record.update(location)
    return data


class AccuWeatherResource(ConfigurableResource):
    """Thin AccuWeather client with retries; the API key is sent as a query parameter and never logged."""

    api_key: str
    base_url: str = "https://dataservice.accuweather.com"
    timeout_seconds: int = 30

    def _session(self) -> requests.Session:
        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def fetch(self, city: City, kind_of_request: str) -> tuple[Any, Optional[str]]:
        validate_kind_of_request(kind_of_request)
        path, params, _ = REQUEST_KINDS[kind_of_request]
        url = self.base_url.rstrip("/") + path.format(accuweather_id=city.accuweather_id)
        with self._session() as session:
            response = session.get(url, params={**params, "apikey": self.api_key}, timeout=self.timeout_seconds)
        if response.status_code != 200:
            try:
                message = response.json().get("Message", response.reason)
            except ValueError:
                message = response.reason
            raise AccuWeatherError(response.status_code, message)
        return response.json(), response.headers.get("RateLimit-Remaining")


class MinioResource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
            config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
        )

    def object_exists(self, s3, key: str) -> bool:
        try:
            s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list_keys(self, s3, prefix: str) -> Iterator[str]:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]

    def read_json(self, s3, key: str) -> Optional[dict]:
        try:
            return json.loads(s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def write_json(self, s3, key: str, payload: Any, metadata: Optional[dict] = None) -> None:
        s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
            Metadata=metadata or {},
        )


LANDING_DDL = """
CREATE SCHEMA IF NOT EXISTS landing;

-- One row per raw JSON file stored in MinIO (API responses and the rebuilt historical files).
CREATE TABLE IF NOT EXISTS landing.accuweather_raw_files (
    object_key            TEXT PRIMARY KEY,
    payload               JSONB NOT NULL,
    source                TEXT NOT NULL,
    data_ingress_date_dte TIMESTAMP NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Snowflake export of HISTORICAL_CONDITION_RAW_DATA_CLONE (point-in-time backup copy).
CREATE TABLE IF NOT EXISTS landing.historical_condition_raw_data_clone_export (
    filename_str          VARCHAR(100),
    payload               JSONB,
    data_ingress_date_dte TIMESTAMP
);

-- Remembers which export files were loaded, so restarts do not reload them.
CREATE TABLE IF NOT EXISTS landing.load_audit (
    source_name TEXT PRIMARY KEY,
    file_name   TEXT NOT NULL,
    file_size   BIGINT NOT NULL,
    file_mtime  BIGINT NOT NULL,
    rows_loaded BIGINT NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class WarehouseResource(ConfigurableResource):
    host: str
    port: int = 5432
    dbname: str
    user: str
    password: str

    @contextmanager
    def connect(self):
        conn = psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password,
            application_name="accuweather_pipeline",
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_landing_schema(self) -> None:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(LANDING_DDL)

    def get_active_cities(self, utc: Optional[int] = None) -> list[City]:
        """Equivalent of get_active_cities_by_utc in the Lambda; utc=None returns every active city."""
        query = """
            SELECT city_accuweather_id_str, city_name_str, country_iso_alpha_2_str, country_name_str,
                   tz_str, utc_int, latitude_flt, longitude_flt, city_folder_str
            FROM stg_accuweather.location_data
            WHERE active_bit AND (%(utc)s::int IS NULL OR utc_int = %(utc)s::int)
            ORDER BY utc_int DESC, country_iso_alpha_2_str, city_name_str
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(query, {"utc": utc})
            return [
                City(
                    accuweather_id=str(row[0]), name=row[1], country_iso_code=row[2], country_name=row[3],
                    tz=row[4], utc=int(row[5]), latitude=float(row[6]), longitude=float(row[7]), folder=row[8],
                )
                for row in cur.fetchall()
            ]


def utc_now_naive() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC")).replace(tzinfo=None)


def pause(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
