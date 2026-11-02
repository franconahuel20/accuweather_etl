"""Daily ingestion: AccuWeather API -> MinIO (raw JSON files) -> PostgreSQL landing table."""
import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from psycopg2.extras import execute_values

from ..resources import (
    CURRENT_CONDITIONS_PREFIX,
    AccuWeatherError,
    AccuWeatherResource,
    MinioResource,
    WarehouseResource,
    build_object_key,
    enrich_with_location,
    pause,
    target_datetime,
    validate_kind_of_request,
)

LOCATION_DATA = AssetKey(["stg_accuweather", "location_data"])
API_RAW_JSON = AssetKey(["minio", "accuweather_api_raw_json"])
HISTORICAL_RAW_JSON = AssetKey(["minio", "historical_export_raw_json"])
LANDING_RAW_FILES = AssetKey(["landing", "accuweather_raw_files"])

SECONDS_BETWEEN_REQUESTS = 1.1  # the API answers with RateLimit-Limit: 1 (request per second)
LANDING_BATCH_SIZE = 200


class FetchConfig(Config):
    kind_of_request: str = "historical_current_conditions"
    utc: Optional[int] = None  # like the Lambda event: only cities with this UTC offset (None = all)
    force: bool = False  # request again even if the file for the target date already exists


@asset(
    key=API_RAW_JSON,
    group_name="ingestion",
    deps=[LOCATION_DATA],
    description=(
        "Requests the last 24 hourly observations of every active city (location_data) from the AccuWeather "
        "API, adds the location fields and stores each response in MinIO as "
        "currentconditions/historical/<country>/<city>/<yyyy>_<Mon>_<dd>_<city>_<country>.json. "
        "A file that already exists is not requested again, so reruns never waste API quota."
    ),
)
def accuweather_api_raw_json(
    context: AssetExecutionContext,
    config: FetchConfig,
    accuweather: AccuWeatherResource,
    minio: MinioResource,
    warehouse: WarehouseResource,
) -> MaterializeResult:
    start_time = time.perf_counter()
    validate_kind_of_request(config.kind_of_request)
    cities = warehouse.get_active_cities(config.utc)
    context.log.info(f"{len(cities)} active cities found for UTC {config.utc if config.utc is not None else 'ALL'}.")

    if not accuweather.api_key.strip():
        context.log.error("ACCUWEATHER_API_KEY is empty: no request was sent. Set it in .env and restart the stack.")
        return MaterializeResult(metadata={"cities": len(cities), "requested": 0, "status": "skipped: no API key"})

    s3 = minio.client()
    requested, uploaded, skipped, observations = 0, 0, 0, 0
    failures: dict[str, str] = {}
    quota_remaining = None

    for city in cities:
        key = build_object_key(config.kind_of_request, city, target_datetime(city))
        if not config.force and minio.object_exists(s3, key):
            skipped += 1
            context.log.info(f"{city.name}, {city.country_name}: {key} already exists, skipping the request.")
            continue

        context.log.info(f"Starting process for {city.name}, {city.country_name}.")
        try:
            data, quota_remaining = accuweather.fetch(city, config.kind_of_request)
            requested += 1
            enrich_with_location(data, city)
            minio.write_json(s3, key, data, metadata={"source": "accuweather-api", "city-accuweather-id": city.accuweather_id})
            uploaded += 1
            observations += len(data) if isinstance(data, list) else 1
            context.log.info(f"Process for {city.name}, {city.country_name} ended: s3://{minio.bucket}/{key}")
        except AccuWeatherError as exc:
            failures[f"{city.name}, {city.country_name}"] = str(exc)
            context.log.error(f"{city.name}, {city.country_name}: AccuWeather request failed ({exc}).")
            if exc.status_code in (401, 403):
                context.log.error("The API key was rejected; the remaining cities are not requested.")
                break
        pause(SECONDS_BETWEEN_REQUESTS)

    elapsed = time.perf_counter() - start_time
    summary = f"{uploaded}/{len(cities) - skipped} cities uploaded to MinIO, {skipped} already present, {len(failures)} failed."
    (context.log.error if failures else context.log.info)(f"{summary} Total time: {elapsed:_.2f} seconds.")
    return MaterializeResult(
        metadata={
            "cities": len(cities),
            "requested": requested,
            "uploaded": uploaded,
            "skipped_existing": skipped,
            "observations": observations,
            "failed": len(failures),
            "failures": MetadataValue.json(failures),
            "api_quota_remaining": quota_remaining or "n/a",
            "elapsed_seconds": round(elapsed, 2),
        }
    )


@asset_check(
    asset=API_RAW_JSON,
    description="Every active city has its raw file for the current target date in MinIO.",
)
def all_active_cities_fetched(minio: MinioResource, warehouse: WarehouseResource) -> AssetCheckResult:
    s3 = minio.client()
    cities = warehouse.get_active_cities()
    missing = [
        f"{city.name}, {city.country_name}"
        for city in cities
        if not minio.object_exists(s3, build_object_key("historical_current_conditions", city, target_datetime(city)))
    ]
    return AssetCheckResult(
        passed=not missing,
        severity=AssetCheckSeverity.ERROR,
        metadata={"active_cities": len(cities), "missing": MetadataValue.json(missing)},
    )


def _download(minio: MinioResource, s3, key: str) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    """Returns (key, payload_json, ingress_date, error)."""
    obj = s3.get_object(Bucket=minio.bucket, Key=key)
    body = obj["Body"].read()
    try:
        payload = json.loads(body)
    except ValueError as exc:
        return key, None, None, f"invalid JSON: {exc}"
    if not isinstance(payload, list):
        return key, None, None, "expected a JSON array of observations"
    return key, json.dumps(payload, ensure_ascii=False), obj.get("Metadata", {}).get("data-ingress-date"), None


@asset(
    key=LANDING_RAW_FILES,
    group_name="landing",
    deps=[API_RAW_JSON, HISTORICAL_RAW_JSON],
    description=(
        "Copies every raw current-conditions file from MinIO that is not yet in PostgreSQL into "
        "landing.accuweather_raw_files (one JSONB row per file). dbt explodes the files into observations."
    ),
)
def landing_accuweather_raw_files(
    context: AssetExecutionContext, minio: MinioResource, warehouse: WarehouseResource
) -> MaterializeResult:
    warehouse.ensure_landing_schema()
    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT object_key FROM landing.accuweather_raw_files")
        loaded = {row[0] for row in cur.fetchall()}

    s3 = minio.client()
    pending = [
        key for key in minio.list_keys(s3, CURRENT_CONDITIONS_PREFIX) if key.endswith(".json") and key not in loaded
    ]
    context.log.info(f"{len(pending)} new files in MinIO ({len(loaded)} already loaded).")

    inserted, rejected = 0, {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for start in range(0, len(pending), LANDING_BATCH_SIZE):
            batch = list(pool.map(lambda key: _download(minio, s3, key), pending[start : start + LANDING_BATCH_SIZE]))
            rows = []
            for key, payload, ingress, error in batch:
                if error:
                    rejected[key] = error
                    continue
                source = "snowflake-export" if ingress else "accuweather-api"
                rows.append((key, payload, source, ingress))
            if rows:
                with warehouse.connect() as conn, conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO landing.accuweather_raw_files (object_key, payload, source, data_ingress_date_dte)
                        VALUES %s
                        ON CONFLICT (object_key) DO NOTHING
                        """,
                        rows,
                        template="(%s, %s::jsonb, %s, COALESCE(%s::timestamp, (now() AT TIME ZONE 'UTC')))",
                        page_size=LANDING_BATCH_SIZE,  # one statement per batch, so rowcount covers it all
                    )
                    inserted += cur.rowcount
            context.log.info(f"Loaded {min(start + LANDING_BATCH_SIZE, len(pending))}/{len(pending)} files.")

    for key, error in rejected.items():
        context.log.error(f"Rejected {key}: {error}")
    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(jsonb_array_length(payload)), 0) FROM landing.accuweather_raw_files")
        total_files, total_observations = cur.fetchone()
    return MaterializeResult(
        metadata={
            "new_files": inserted,
            "rejected_files": MetadataValue.json(rejected),
            "total_files": total_files,
            "total_observations": int(total_observations),
        }
    )
