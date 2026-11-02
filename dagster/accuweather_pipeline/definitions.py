"""Dagster code location: assets, jobs, the daily schedule and resources."""
import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from .assets import (
    accuweather_api_raw_json,
    all_active_cities_fetched,
    dbt_models,
    dbt_project,
    dbt_seeds,
    historical_condition_raw_data_clone_export,
    historical_export_raw_json,
    landing_accuweather_raw_files,
)
from .resources import AccuWeatherResource, MinioResource, WarehouseResource

historical = AssetSelection.groups("historical_backfill")

# Daily refresh: seeds -> AccuWeather API -> MinIO -> landing -> dbt (raw table, view and marts).
accuweather_daily_job = define_asset_job(
    name="accuweather_daily_job",
    selection=AssetSelection.all() - historical,
    description="Fetches yesterday's observations for every active city and refreshes the view and the marts.",
)

# Loads the Snowflake exports (if present) and rebuilds everything downstream.
historical_backfill_job = define_asset_job(
    name="historical_backfill_job",
    selection=historical | AssetSelection.assets(dbt_seeds, landing_accuweather_raw_files, dbt_models),
    description="One-time load of the historical Snowflake exports placed in HISTORICAL_DATA_DIR.",
)

# Executed by the pipeline-bootstrap container on `docker compose up -d`; every step is idempotent.
bootstrap_job = define_asset_job(
    name="bootstrap_job",
    selection=AssetSelection.all(),
    description="Historical backfill (once) + daily ingestion (once per day) + dbt build.",
)

daily_schedule = ScheduleDefinition(
    name="accuweather_daily_schedule",
    job=accuweather_daily_job,
    cron_schedule=os.environ.get("PIPELINE_SCHEDULE_CRON", "30 8 * * *"),
    execution_timezone=os.environ.get("PIPELINE_SCHEDULE_TIMEZONE", "UTC"),
    default_status=DefaultScheduleStatus.RUNNING,
    description="Runs after local midnight in every monitored time zone (UTC-3 to UTC-8).",
)

defs = Definitions(
    assets=[
        dbt_seeds,
        accuweather_api_raw_json,
        historical_export_raw_json,
        historical_condition_raw_data_clone_export,
        landing_accuweather_raw_files,
        dbt_models,
    ],
    asset_checks=[all_active_cities_fetched],
    jobs=[accuweather_daily_job, historical_backfill_job, bootstrap_job],
    schedules=[daily_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        "accuweather": AccuWeatherResource(
            api_key=EnvVar("ACCUWEATHER_API_KEY"),
            base_url=os.environ.get("ACCUWEATHER_BASE_URL", "https://dataservice.accuweather.com"),
        ),
        "minio": MinioResource(
            endpoint_url=EnvVar("MINIO_ENDPOINT"),
            access_key=EnvVar("MINIO_ROOT_USER"),
            secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
            bucket=EnvVar("MINIO_BUCKET"),
        ),
        "warehouse": WarehouseResource(
            host=EnvVar("WAREHOUSE_HOST"),
            port=int(os.environ.get("WAREHOUSE_PORT", "5432")),
            dbname=EnvVar("WAREHOUSE_DB"),
            user=EnvVar("WAREHOUSE_USER"),
            password=EnvVar("WAREHOUSE_PASSWORD"),
        ),
    },
)
