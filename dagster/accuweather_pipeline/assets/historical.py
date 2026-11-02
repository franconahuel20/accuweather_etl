"""One-time historical backfill from the Snowflake exports (CSV) placed in HISTORICAL_DATA_DIR.

* historical_condition_raw_data.csv  -> the original raw JSON files are rebuilt in MinIO with their
  original keys, so history and new API files follow exactly the same path into the warehouse.
* historical_condition_raw_data_clone.csv -> landing.historical_condition_raw_data_clone_export (COPY).

Both steps remember what they loaded (MinIO marker / landing.load_audit) and skip unchanged files.
"""
import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from ..resources import CURRENT_CONDITIONS_PREFIX, MinioResource, WarehouseResource
from .ingestion import HISTORICAL_RAW_JSON

RAW_EXPORT_FILE = "historical_condition_raw_data.csv"
CLONE_EXPORT_FILE = "historical_condition_raw_data_clone.csv"
RAW_EXPORT_MARKER = "_control/historical_export_raw_json.json"
CLONE_EXPORT = AssetKey(["landing", "historical_condition_raw_data_clone_export"])

csv.field_size_limit(sys.maxsize)


def _historical_dir() -> Path:
    return Path(os.environ.get("HISTORICAL_DATA_PATH", "/opt/accuweather/historical"))


def _file_signature(path: Path) -> dict:
    stat = path.stat()
    return {"file_name": path.name, "file_size": stat.st_size, "file_mtime": int(stat.st_mtime)}


@asset(
    key=HISTORICAL_RAW_JSON,
    group_name="historical_backfill",
    description=(
        "Rebuilds the original raw JSON files (one per city and day) from the Snowflake export of "
        "HISTORICAL_CONDITION_RAW_DATA and stores them in MinIO under their original keys, keeping the original "
        "DATA_INGRESS_DATE_DTE as object metadata. Runs once; skipped when the export is absent or unchanged."
    ),
)
def historical_export_raw_json(context: AssetExecutionContext, minio: MinioResource) -> MaterializeResult:
    path = _historical_dir() / RAW_EXPORT_FILE
    if not path.exists():
        context.log.info(f"{path} not found: historical backfill skipped (the pipeline works without it).")
        return MaterializeResult(metadata={"status": "skipped: export not found"})

    s3 = minio.client()
    signature = _file_signature(path)
    marker = minio.read_json(s3, RAW_EXPORT_MARKER)
    if marker and all(marker.get(k) == v for k, v in signature.items()):
        context.log.info(f"{path.name} was already backfilled ({marker.get('files')} files); nothing to do.")
        return MaterializeResult(metadata={"status": "skipped: already loaded", **marker})

    existing = set(minio.list_keys(s3, CURRENT_CONDITIONS_PREFIX))
    context.log.info(f"Rebuilding raw files from {path.name}; {len(existing)} files already in MinIO.")

    stats = {"files": 0, "uploaded": 0, "already_present": 0, "observations": 0}
    flushed: set[str] = set()

    def upload(key: str, records: list[str], ingress: str) -> None:
        body = ("[" + ",".join(records) + "]").encode("utf-8")
        s3.put_object(
            Bucket=minio.bucket, Key=key, Body=body, ContentType="application/json",
            Metadata={"source": "snowflake-export", "data-ingress-date": ingress},
        )

    with ThreadPoolExecutor(max_workers=8) as pool, open(path, newline="", encoding="utf-8") as handle:
        futures = []

        def flush(key: str, records: list[str], ingress: str) -> None:
            if key in flushed:
                raise ValueError(f"Rows of {key} are not contiguous in {path.name}; export it ordered by FILENAME_STR.")
            flushed.add(key)
            stats["files"] += 1
            stats["observations"] += len(records)
            if key in existing:
                stats["already_present"] += 1
                return
            futures.append(pool.submit(upload, key, records, ingress))
            stats["uploaded"] += 1
            if len(futures) >= 64:
                for future in futures:
                    future.result()
                futures.clear()
            if stats["files"] % 1000 == 0:
                context.log.info(f"{stats['files']} files processed...")

        reader = csv.reader(handle)
        header = next(reader)
        if header[:3] != ["FILENAME_STR", "PAYLOAD", "DATA_INGRESS_DATE_DTE"]:
            raise ValueError(f"Unexpected header in {path.name}: {header}")

        current_key, records, ingress = None, [], ""
        for filename, payload, ingress_date in reader:
            if filename != current_key:
                if current_key is not None:
                    flush(current_key, records, ingress)
                current_key, records, ingress = filename, [], ingress_date
            # Compact the pretty-printed JSON exported by Snowflake.
            records.append(json.dumps(json.loads(payload), ensure_ascii=False, separators=(",", ":")))
            ingress = min(ingress, ingress_date)
        if current_key is not None:
            flush(current_key, records, ingress)
        wait(futures)
        for future in futures:
            future.result()

    minio.write_json(s3, RAW_EXPORT_MARKER, {**signature, **stats})
    context.log.info(
        f"Historical backfill done: {stats['files']} files ({stats['observations']} observations), "
        f"{stats['uploaded']} uploaded, {stats['already_present']} already present."
    )
    return MaterializeResult(metadata=stats)


@asset(
    key=CLONE_EXPORT,
    group_name="historical_backfill",
    description=(
        "Loads the Snowflake export of HISTORICAL_CONDITION_RAW_DATA_CLONE (a point-in-time backup copy of the raw "
        "table) into landing with COPY. Reloaded only when the export file changes."
    ),
)
def historical_condition_raw_data_clone_export(
    context: AssetExecutionContext, warehouse: WarehouseResource
) -> MaterializeResult:
    warehouse.ensure_landing_schema()
    path = _historical_dir() / CLONE_EXPORT_FILE
    if not path.exists():
        context.log.info(f"{path} not found: the clone table stays empty.")
        return MaterializeResult(metadata={"status": "skipped: export not found"})

    signature = _file_signature(path)
    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT file_size, file_mtime, rows_loaded FROM landing.load_audit WHERE source_name = %s",
            (CLONE_EXPORT.path[-1],),
        )
        audit = cur.fetchone()
        if audit and audit[0] == signature["file_size"] and audit[1] == signature["file_mtime"]:
            context.log.info(f"{path.name} already loaded ({audit[2]} rows); nothing to do.")
            return MaterializeResult(metadata={"status": "skipped: already loaded", "rows": audit[2]})

        cur.execute("TRUNCATE landing.historical_condition_raw_data_clone_export")
        with open(path, encoding="utf-8") as handle:
            cur.copy_expert(
                "COPY landing.historical_condition_raw_data_clone_export (filename_str, payload, data_ingress_date_dte) "
                "FROM STDIN WITH (FORMAT csv, HEADER true)",
                handle,
            )
        rows = cur.rowcount
        cur.execute(
            """
            INSERT INTO landing.load_audit (source_name, file_name, file_size, file_mtime, rows_loaded)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET file_name = EXCLUDED.file_name, file_size = EXCLUDED.file_size,
                file_mtime = EXCLUDED.file_mtime, rows_loaded = EXCLUDED.rows_loaded, loaded_at = now()
            """,
            (CLONE_EXPORT.path[-1], signature["file_name"], signature["file_size"], signature["file_mtime"], rows),
        )
    context.log.info(f"Loaded {rows} rows from {path.name}.")
    return MaterializeResult(metadata={"rows": rows, **signature})
