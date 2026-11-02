"""dbt project exposed as Dagster assets. Seeds and models are two separate steps because the API
ingestion runs in between: it reads location_data (seed) and feeds the raw table (model)."""
import os
from pathlib import Path
from typing import Any, Mapping, Optional

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", Path(__file__).resolve().parents[3] / "dbt"))

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR))


class AccuWeatherDbtTranslator(DagsterDbtTranslator):
    """Asset keys follow <schema>/<table>, matching the PostgreSQL objects (landing sources included)."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([dbt_resource_props["source_name"], dbt_resource_props["name"]])
        schema = dbt_resource_props.get("config", {}).get("schema") or "stg_accuweather"
        return AssetKey([schema, dbt_resource_props["name"]])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        schema = dbt_resource_props.get("config", {}).get("schema") or "stg_accuweather"
        return "marts" if schema == "analytics" else "warehouse"


translator = AccuWeatherDbtTranslator()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="resource_type:seed",
    name="dbt_seeds",
    dagster_dbt_translator=translator,
)
def dbt_seeds(context: AssetExecutionContext, dbt: DbtCliResource):
    # "cautious": tests that also read models (e.g. active cities vs. latest conditions) run with the models.
    yield from dbt.cli(["build", "--indirect-selection", "cautious"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    exclude="resource_type:seed",
    name="dbt_models",
    dagster_dbt_translator=translator,
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
