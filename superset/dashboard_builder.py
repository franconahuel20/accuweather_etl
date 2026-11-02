"""Builds the "AccuWeather - Global Weather Monitor" dashboard through the Superset REST API and exports it
as the bundle in superset/dashboards/, which the Superset container imports on a fresh installation.

Dashboards as code: edit the chart definitions below and run (with the stack up):

    python superset/dashboard_builder.py --url http://localhost:8089 --env .env

Requires `requests` and `pyyaml` on the host. Re-running replaces the dashboard and its charts.
"""
import argparse
import io
import json
import shutil
import sys
import uuid
import zipfile
from pathlib import Path

import requests
import yaml

DASHBOARD_SLUG = "accuweather-weather"
DASHBOARD_TITLE = "AccuWeather · Global Weather Monitor"
DATABASE_NAME = "Weather Warehouse (PostgreSQL)"
BUNDLE_DIR = Path(__file__).resolve().parent / "dashboards" / "accuweather_weather"

BASE_MAP_LIGHT_GRAY = (
    "tile://https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}"
)

# Weekly series with a gap (e.g. between the historical backfill and the first daily load) must break,
# not draw a straight line across months without data: resample to 7 days and keep the gaps empty.
BREAK_LINE_ON_GAPS = {"resample_rule": "7D", "resample_method": "asfreq"}

LABEL_COLORS = {
    "Maximum (°C)": "#D7263D", "Mean (°C)": "#F08A3C", "Minimum (°C)": "#3D8BD9",
    "Clear & sunny": "#F2C14E", "Partly cloudy": "#9BB7D4", "Cloudy": "#7A869A", "Rain & drizzle": "#3D8BD9",
    "Thunderstorms": "#5B3E96", "Fog, mist & haze": "#B9B9B9", "Snow & ice": "#BFE6FF", "Other": "#E0E0E0",
}

TEMPERATURE_BAND_COLORS = {
    "1. Freezing (< 0 °C)": "#1F4E99",
    "2. Cold (0-10 °C)": "#3D8BD9",
    "3. Cool (10-18 °C)": "#5BC0BE",
    "4. Mild (18-25 °C)": "#F2C14E",
    "5. Warm (25-32 °C)": "#F08A3C",
    "6. Hot (>= 32 °C)": "#D7263D",
}

# Dataset name -> (schema, table, column verbose names)
DATASETS = {
    "view": (
        "stg_accuweather",
        "historical_condition_data_view",
        {
            "city_name": "City", "country": "Country", "country_code": "Country code",
            "local_obs_datetime": "Observation time", "weather_text": "Conditions", "temp_c": "Temperature (°C)",
            "real_feel_temp_c": "RealFeel (°C)", "relative_humidity": "Relative humidity (%)",
            "pressure_metric_mb": "Pressure (mb)", "uv_index": "UV index", "uv_index_text": "UV risk",
        },
    ),
    "latest": (
        "analytics",
        "city_latest_conditions",
        {
            "city_label": "City", "city_name": "City name", "country": "Country", "country_code": "Country code",
            "weather_text": "Conditions", "observed_at_local": "Observed (local time)", "observed_at": "Observed at",
            "temperature_band": "Temperature band", "uv_index_text": "UV risk", "wind_direction": "Wind direction",
        },
    ),
    "daily": (
        "analytics",
        "city_daily_weather",
        {
            "city_label": "City", "city_name": "City name", "country": "Country", "local_date": "Date",
            "local_month": "Month", "prevailing_weather": "Prevailing conditions",
        },
    ),
    "hourly": (
        "analytics",
        "weather_observations",
        {
            "city_label": "City", "city_name": "City name", "country": "Country", "local_hour": "Local hour",
            "local_date": "Date", "weather_text": "Conditions", "weather_group": "Condition group", "uv_index_text": "UV risk",
        },
    ),
}


def metric(column: str, aggregate: str, label: str) -> dict:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label,
        "hasCustomLabel": True,
        "optionName": f"metric_{uuid.uuid4().hex[:12]}",
    }


def sql_metric(expression: str, label: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "hasCustomLabel": True,
        "optionName": f"metric_{uuid.uuid4().hex[:12]}",
    }


def build_charts() -> list[dict]:
    """Each chart: key, dataset, name, viz_type, params, description."""
    temp_now = sql_metric("ROUND(AVG(temp_c), 1)", "Temperature (°C)")
    charts = [
        # ---------------- Tab 1: current conditions ----------------
        {
            "key": "kpi_cities", "dataset": "latest", "name": "Cities monitored", "viz_type": "big_number_total",
            "params": {"metric": sql_metric("COUNT(DISTINCT accuweather_id)", "Cities"), "subheader": "with live AccuWeather data",
                       "subtitle": "with live AccuWeather data", "y_axis_format": ",d", "header_font_size": 0.4,
                       "subheader_font_size": 0.15, "subtitle_font_size": 0.15},
        },
        {
            "key": "kpi_avg_temp", "dataset": "latest", "name": "Average temperature now", "viz_type": "big_number_total",
            "params": {"metric": sql_metric("ROUND(AVG(temp_c), 1)", "Avg °C"), "subheader": "°C, mean of the latest reading per city",
                       "subtitle": "°C, mean of the latest reading per city", "y_axis_format": ".1f", "header_font_size": 0.4,
                       "subheader_font_size": 0.15, "subtitle_font_size": 0.15},
        },
        {
            "key": "kpi_max_temp", "dataset": "latest", "name": "Warmest city now", "viz_type": "big_number_total",
            "params": {"metric": sql_metric("MAX(temp_c)", "Max °C"), "subheader": "°C, highest current temperature",
                       "subtitle": "°C, highest current temperature", "y_axis_format": ".1f", "header_font_size": 0.4,
                       "subheader_font_size": 0.15, "subtitle_font_size": 0.15},
        },
        {
            "key": "kpi_min_temp", "dataset": "latest", "name": "Coldest city now", "viz_type": "big_number_total",
            "params": {"metric": sql_metric("MIN(temp_c)", "Min °C"), "subheader": "°C, lowest current temperature",
                       "subtitle": "°C, lowest current temperature", "y_axis_format": ".1f", "header_font_size": 0.4,
                       "subheader_font_size": 0.15, "subtitle_font_size": 0.15},
        },
        {
            "key": "world_map", "dataset": "latest", "name": "World map · average temperature by country", "viz_type": "world_map",
            "description": "Countries coloured by the mean of the latest temperature of their cities; bubble size = cities monitored.",
            "params": {"entity": "country_code", "country_fieldtype": "cca2", "metric": temp_now,
                       "secondary_metric": sql_metric("COUNT(DISTINCT accuweather_id)", "Cities monitored"),
                       "show_bubbles": True, "max_bubble_size": "10", "color_by": "metric",
                       "linear_color_scheme": "schemeYlOrRd", "row_limit": 10000, "y_axis_format": ".1f"},
        },
        {
            "key": "city_map", "dataset": "latest", "name": "Cities map · current temperature band", "viz_type": "deck_scatter",
            "description": "Each city at its latitude/longitude, coloured by its current temperature band.",
            "params": {"spatial": {"type": "latlong", "latCol": "latitude", "lonCol": "longitude"},
                       "mapbox_style": BASE_MAP_LIGHT_GRAY,
                       "viewport": {"longitude": -78.0, "latitude": -8.0, "zoom": 2.1, "bearing": 0, "pitch": 0},
                       # radius in km, but never smaller than 7 px (radiusMinPixels) so cities stay visible at world zoom
                       "autozoom": True, "point_radius_fixed": {"type": "fix", "value": "60"}, "point_unit": "radius_km",
                       "min_radius": 7, "max_radius": 40, "multiplier": 1, "dimension": "temperature_band",
                       "color_scheme": "supersetColors", "color_picker": {"r": 215, "g": 38, "b": 61, "a": 1},
                       "legend_position": "tr", "row_limit": 5000},
        },
        {
            "key": "current_table", "dataset": "latest", "name": "Current conditions by city", "viz_type": "table",
            "params": {"query_mode": "aggregate", "groupby": ["city_label", "weather_text", "observed_at_local"],
                       "metrics": [sql_metric("MAX(temp_c)", "Temp (°C)"), sql_metric("MAX(real_feel_temp_c)", "RealFeel (°C)"),
                                   sql_metric("MAX(temp_min_24h_c)", "24h min (°C)"), sql_metric("MAX(temp_max_24h_c)", "24h max (°C)"),
                                   sql_metric("MAX(relative_humidity)", "Humidity (%)"), sql_metric("MAX(wind_speed_kmh)", "Wind (km/h)"),
                                   sql_metric("MAX(uv_index)", "UV index"), sql_metric("MAX(precipitation_24h_mm)", "Rain 24h (mm)")],
                       "timeseries_limit_metric": sql_metric("MAX(temp_c)", "Temp (°C)"), "order_desc": True,
                       "row_limit": 100, "server_page_length": 20, "include_search": True, "show_cell_bars": False,
                       "table_timestamp_format": "%Y-%m-%d %H:%M", "color_pn": False,
                       "column_config": {"Temp (°C)": {"d3NumberFormat": ".1f"}, "RealFeel (°C)": {"d3NumberFormat": ".1f"},
                                         "24h min (°C)": {"d3NumberFormat": ".1f"}, "24h max (°C)": {"d3NumberFormat": ".1f"}}},
        },
        {
            "key": "temp_vs_realfeel", "dataset": "latest", "name": "Temperature vs RealFeel® now", "viz_type": "echarts_timeseries_bar",
            "description": "Gap between measured and perceived temperature (wind, humidity, sun).",
            "params": {"x_axis": "city_label", "metrics": [sql_metric("MAX(temp_c)", "Temperature (°C)"),
                                                            sql_metric("MAX(real_feel_temp_c)", "RealFeel (°C)")],
                       "groupby": [], "orientation": "horizontal", "x_axis_sort": "Temperature (°C)", "x_axis_sort_asc": True,
                       "row_limit": 50, "show_legend": True, "legendOrientation": "top", "y_axis_format": ".1f",
                       "show_value": False, "rich_tooltip": True, "color_scheme": "supersetColors", "time_range": "No filter"},
        },
        {
            "key": "last_week_hourly", "dataset": "view", "name": "Last 7 days · hourly temperature by country (daily feed)",
            "viz_type": "echarts_timeseries_line",
            "description": "Straight from HISTORICAL_CONDITION_DATA_VIEW, refreshed every day by Dagster.",
            "params": {"x_axis": "local_obs_datetime", "time_grain_sqla": "PT1H", "metrics": [temp_now],
                       "groupby": ["country"], "time_range": "Last week", "granularity_sqla": "local_obs_datetime",
                       "row_limit": 10000, "show_legend": True, "legendOrientation": "top", "rich_tooltip": True,
                       "y_axis_format": ".1f", "x_axis_time_format": "smart_date", "markerEnabled": False,
                       "zoomable": True, "color_scheme": "supersetColors", "truncateYAxis": True},
        },
        # ---------------- Tab 2: trends & climate ----------------
        {
            "key": "weekly_temp_country", "dataset": "daily", "name": "Weekly average temperature by country", "viz_type": "echarts_timeseries_line",
            "params": {"x_axis": "local_date", "time_grain_sqla": "P1W", "metrics": [sql_metric("ROUND(AVG(temp_avg_c), 1)", "Avg temperature (°C)")],
                       "groupby": ["country"], "time_range": "No filter", "row_limit": 10000, "show_legend": True,
                       "legendOrientation": "top", "rich_tooltip": True, "y_axis_format": ".1f", "x_axis_time_format": "smart_date",
                       "markerEnabled": False, "zoomable": True, "color_scheme": "supersetColors", "truncateYAxis": True,
                       **BREAK_LINE_ON_GAPS},
        },
        {
            "key": "temp_envelope", "dataset": "daily", "name": "Temperature envelope · weekly min / mean / max", "viz_type": "echarts_timeseries_line",
            "description": "Pick a city or country in the filters to see its own range.",
            "params": {"x_axis": "local_date", "time_grain_sqla": "P1W",
                       "metrics": [sql_metric("MIN(temp_min_c)", "Minimum (°C)"), sql_metric("ROUND(AVG(temp_avg_c), 1)", "Mean (°C)"),
                                   sql_metric("MAX(temp_max_c)", "Maximum (°C)")],
                       "groupby": [], "time_range": "No filter", "row_limit": 10000, "show_legend": True, "legendOrientation": "top",
                       "rich_tooltip": True, "y_axis_format": ".1f", "x_axis_time_format": "smart_date", "markerEnabled": False,
                       "zoomable": True, "color_scheme": "supersetColors", "truncateYAxis": True, **BREAK_LINE_ON_GAPS},
        },
        {
            "key": "diurnal_heatmap", "dataset": "hourly", "name": "Daily cycle · average temperature by local hour", "viz_type": "heatmap_v2",
            "description": "When each city is warmest and coldest, in its own local time.",
            "params": {"x_axis": "local_hour", "groupby": "city_label", "metric": sql_metric("ROUND(AVG(temp_c), 1)", "Avg temperature (°C)"),
                       "linear_color_scheme": "schemeYlOrRd", "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
                       "normalize_across": "heatmap", "legend_type": "continuous", "show_values": False, "show_percentage": False,
                       "row_limit": 10000, "y_axis_format": ".1f", "time_range": "No filter", "xscale_interval": -1,
                       "yscale_interval": -1, "left_margin": "auto", "bottom_margin": "auto", "value_bounds": [None, None]},
        },
        {
            "key": "temp_distribution", "dataset": "daily", "name": "Distribution of daily mean temperature by city", "viz_type": "box_plot",
            "description": "Box = middle 50% of days; whiskers = Tukey (1.5 IQR); dots = unusual days.",
            "params": {"columns": ["local_date"], "groupby": ["city_label"], "metrics": [sql_metric("MAX(temp_avg_c)", "Daily mean (°C)")],
                       "whiskerOptions": "Tukey", "time_range": "No filter", "row_limit": 100000, "x_ticks_layout": "45°",
                       "number_format": ".1f", "color_scheme": "supersetColors"},
        },
        {
            "key": "climate_profile", "dataset": "daily", "name": "Climate profile · temperature vs humidity", "viz_type": "bubble_v2",
            "description": "Each bubble is a city: warm/humid cities sit top-right; bigger bubbles = larger day-night swings.",
            "params": {"entity": "city_label", "series": "country", "x": sql_metric("ROUND(AVG(temp_avg_c), 1)", "Avg temperature (°C)"),
                       "y": sql_metric("ROUND(AVG(humidity_avg), 1)", "Avg relative humidity (%)"),
                       "size": sql_metric("ROUND(AVG(temp_range_c), 1)", "Avg daily range (°C)"), "max_bubble_size": "12", "row_limit": 1000,
                       "x_axis_label": "Average temperature (°C)", "y_axis_label": "Average relative humidity (%)",
                       "xAxisFormat": ".1f", "y_axis_format": ".0f", "show_legend": True, "legendOrientation": "top",
                       "color_scheme": "supersetColors", "time_range": "No filter", "opacity": 0.6},
        },
        {
            "key": "rain_hours", "dataset": "daily", "name": "Hours with precipitation per month", "viz_type": "echarts_timeseries_bar",
            "params": {"x_axis": "local_month", "time_grain_sqla": "P1M", "metrics": [sql_metric("SUM(hours_with_precipitation)", "Hours with precipitation")],
                       "groupby": ["country"], "stack": "Stack", "time_range": "No filter", "row_limit": 10000, "show_legend": True,
                       "legendOrientation": "top", "rich_tooltip": True, "y_axis_format": ",d", "x_axis_time_format": "%b %Y",
                       "color_scheme": "supersetColors", "zoomable": True},
        },
        {
            "key": "conditions_share", "dataset": "hourly", "name": "Weather conditions mix (share of observed hours)", "viz_type": "pie",
            "description": "AccuWeather's ~35 condition phrases grouped into comparable categories.",
            "params": {"groupby": ["weather_group"], "metric": sql_metric("COUNT(*)", "Hours"), "donut": True, "show_labels": True,
                       "labels_outside": True, "label_type": "percent", "row_limit": 20, "sort_by_metric": True,
                       "show_legend": True, "legendOrientation": "right", "innerRadius": 40, "outerRadius": 72, "show_total": True, "number_format": ",d",
                       "color_scheme": "supersetColors", "time_range": "No filter"},
        },
        {
            "key": "uv_by_city", "dataset": "daily", "name": "Average daily maximum UV index by city", "viz_type": "echarts_timeseries_bar",
            "description": "UV index 0-2 low, 3-5 moderate, 6-7 high, 8-10 very high, 11+ extreme.",
            "params": {"x_axis": "city_label", "metrics": [sql_metric("ROUND(AVG(uv_index_max), 1)", "Avg daily max UV")],
                       "groupby": [], "orientation": "vertical", "x_axis_sort": "Avg daily max UV", "x_axis_sort_asc": False,
                       "row_limit": 50, "show_legend": False, "y_axis_format": ".1f", "show_value": True, "rich_tooltip": True,
                       "color_scheme": "supersetColors", "time_range": "No filter", "xAxisLabelRotation": 30},
        },
    ]
    return charts


MARKDOWN_NOW = """### Current weather across the monitored cities
Latest AccuWeather observation of every active city in `location_data`. Data lands daily at **08:30 UTC**
(AccuWeather API → MinIO → PostgreSQL → dbt), orchestrated by Dagster. Use the **Country** and **City**
filters on the left; maps and tables react to them.
"""

MARKDOWN_TRENDS = """### Trends and climate patterns
Historical hourly observations (Aug 2023 – Oct 2024 backfill + every daily load). Narrow the period with the
**Date range** filter. Weekly grain smooths day-to-day noise; the heatmap uses each city's own local time.
"""


class Superset:
    def __init__(self, url: str, username: str, password: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()
        token = self._request("POST", "/api/v1/security/login",
                              json={"username": username, "password": password, "provider": "db", "refresh": True})
        self.session.headers["Authorization"] = f"Bearer {token['access_token']}"
        csrf = self._request("GET", "/api/v1/security/csrf_token/")["result"]
        self.session.headers.update({"X-CSRFToken": csrf, "Referer": self.url})

    def _request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, self.url + path, timeout=120, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:800]}")
        return response.json() if response.content and "json" in response.headers.get("Content-Type", "") else response

    def get(self, path, **kw): return self._request("GET", path, **kw)
    def post(self, path, payload): return self._request("POST", path, json=payload)
    def put(self, path, payload): return self._request("PUT", path, json=payload)
    def delete(self, path): return self._request("DELETE", path)

    def list_all(self, resource: str) -> list[dict]:
        return self.get(f"/api/v1/{resource}/", params={"q": "(page_size:100)"})["result"]


def read_env(path: Path) -> dict:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def ensure_database(api: Superset, env: dict) -> int:
    uri = "postgresql+psycopg2://{u}:{p}@postgres:5432/{db}".format(
        u=env.get("BI_READER_USER", "bi_reader"), p=env["BI_READER_PASSWORD"], db=env.get("WAREHOUSE_DB", "weather"))
    for database in api.list_all("database"):
        if database["database_name"] == DATABASE_NAME:
            api.put(f"/api/v1/database/{database['id']}", {"sqlalchemy_uri": uri})
            return database["id"]
    created = api.post("/api/v1/database/", {"database_name": DATABASE_NAME, "sqlalchemy_uri": uri,
                                             "expose_in_sqllab": True, "allow_dml": False,
                                             "extra": json.dumps({"allows_virtual_table_explore": True})})
    return created["id"]


COLUMN_KEYS = ("id", "column_name", "verbose_name", "description", "expression", "filterable", "groupby",
               "is_active", "is_dttm", "python_date_format", "type", "uuid", "extra", "advanced_data_type")


def ensure_dataset(api: Superset, database_id: int, schema: str, table: str, verbose: dict) -> int:
    existing = [d for d in api.list_all("dataset") if d["table_name"] == table and d["schema"] == schema]
    dataset_id = existing[0]["id"] if existing else api.post(
        "/api/v1/dataset/", {"database": database_id, "schema": schema, "table_name": table})["id"]
    api.put(f"/api/v1/dataset/{dataset_id}/refresh", {})
    detail = api.get(f"/api/v1/dataset/{dataset_id}")["result"]
    columns = []
    for column in detail["columns"]:
        item = {k: column.get(k) for k in COLUMN_KEYS if k in column}
        if column["column_name"] in verbose:
            item["verbose_name"] = verbose[column["column_name"]]
        columns.append({k: v for k, v in item.items() if v is not None})
    dttm = next((c["column_name"] for c in detail["columns"] if c.get("is_dttm")), None)
    api.put(f"/api/v1/dataset/{dataset_id}", {"columns": columns, "main_dttm_col": dttm})
    return dataset_id


def chart_node(chart_id: int, chart_uuid: str, name: str, width: int, height: int, parents: list[str]) -> tuple[str, dict]:
    node_id = f"CHART-{chart_uuid[:8]}"
    return node_id, {"type": "CHART", "id": node_id, "children": [], "parents": parents,
                     "meta": {"chartId": chart_id, "uuid": chart_uuid, "width": width, "height": height, "sliceName": name}}


def build_layout(charts: dict) -> dict:
    """charts: key -> (id, uuid, name). Returns position_json."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS-main"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
        "TABS-main": {"type": "TABS", "id": "TABS-main", "children": ["TAB-now", "TAB-trends"], "parents": ["ROOT_ID"]},
    }
    tabs = {
        "TAB-now": ("Current conditions", [
            [("MARKDOWN-now", 12, 18)],
            [("kpi_cities", 3, 28), ("kpi_avg_temp", 3, 28), ("kpi_max_temp", 3, 28), ("kpi_min_temp", 3, 28)],
            [("world_map", 6, 92), ("city_map", 6, 92)],
            [("current_table", 8, 80), ("temp_vs_realfeel", 4, 80)],
            [("last_week_hourly", 12, 60)],
        ]),
        "TAB-trends": ("Trends & climate", [
            [("MARKDOWN-trends", 12, 18)],
            [("weekly_temp_country", 6, 64), ("temp_envelope", 6, 64)],
            [("diurnal_heatmap", 7, 90), ("climate_profile", 5, 90)],
            [("temp_distribution", 12, 70)],
            [("rain_hours", 6, 64), ("conditions_share", 6, 64)],
            [("uv_by_city", 12, 60)],
        ]),
    }
    for tab_id, (title, rows) in tabs.items():
        tab_parents = ["ROOT_ID", "TABS-main"]
        layout[tab_id] = {"type": "TAB", "id": tab_id, "children": [], "parents": tab_parents,
                          "meta": {"text": title, "defaultText": "Tab title", "placeholder": "Tab title"}}
        for index, row in enumerate(rows):
            row_id = f"ROW-{tab_id[4:]}-{index}"
            row_parents = tab_parents + [tab_id]
            layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": row_parents,
                              "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout[tab_id]["children"].append(row_id)
            for key, width, height in row:
                parents = row_parents + [row_id]
                if key.startswith("MARKDOWN"):
                    code = MARKDOWN_NOW if key.endswith("now") else MARKDOWN_TRENDS
                    layout[key] = {"type": "MARKDOWN", "id": key, "children": [], "parents": parents,
                                   "meta": {"width": width, "height": height, "code": code}}
                    node_id = key
                else:
                    chart_id, chart_uuid, name = charts[key]
                    node_id, node = chart_node(chart_id, chart_uuid, name, width, height, parents)
                    layout[node_id] = node
                layout[row_id]["children"].append(node_id)
    return layout


def native_filters(datasets: dict, charts: dict) -> list[dict]:
    now_only = [charts[k][0] for k in ("kpi_cities", "kpi_avg_temp", "kpi_max_temp", "kpi_min_temp", "world_map",
                                       "city_map", "current_table", "temp_vs_realfeel", "last_week_hourly")]
    select_controls = {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
                       "searchAllOptions": False, "inverseSelection": False}
    empty_mask = {"extraFormData": {}, "filterState": {}, "ownState": {}}
    return [
        {"id": "NATIVE_FILTER-country", "name": "Country", "filterType": "filter_select", "type": "NATIVE_FILTER",
         "targets": [{"datasetId": datasets["latest"], "column": {"name": "country"}}],
         "controlValues": select_controls, "defaultDataMask": empty_mask, "cascadeParentIds": [],
         "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "description": "Filter every chart by country."},
        {"id": "NATIVE_FILTER-city", "name": "City", "filterType": "filter_select", "type": "NATIVE_FILTER",
         "targets": [{"datasetId": datasets["latest"], "column": {"name": "city_name"}}],
         "controlValues": select_controls, "defaultDataMask": empty_mask, "cascadeParentIds": ["NATIVE_FILTER-country"],
         "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "description": "Filter every chart by city."},
        {"id": "NATIVE_FILTER-period", "name": "Date range", "filterType": "filter_time", "type": "NATIVE_FILTER",
         "targets": [{}], "controlValues": {"enableEmptyFilter": False},
         "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}}, "cascadeParentIds": [],
         "scope": {"rootPath": ["ROOT_ID"], "excluded": now_only},
         "description": "Period of the historical charts (Trends & climate tab)."},
    ]


def export_bundle(api: Superset, dashboard_id: int) -> None:
    response = api.session.get(f"{api.url}/api/v1/dashboard/export/", params={"q": f"!({dashboard_id})"}, timeout=120)
    response.raise_for_status()
    if BUNDLE_DIR.exists():
        shutil.rmtree(BUNDLE_DIR)
    with zipfile.ZipFile(io.BytesIO(response.content)) as bundle:
        for name in bundle.namelist():
            relative = name.split("/", 1)[1]
            target = BUNDLE_DIR / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            content = bundle.read(name)
            if relative.startswith("databases/"):
                config = yaml.safe_load(content)
                # The real connection string is injected at import time from .env.
                config["sqlalchemy_uri"] = "postgresql+psycopg2://bi_reader:XXXXXXXXXX@postgres:5432/weather"
                content = yaml.safe_dump(config, sort_keys=False, allow_unicode=True).encode("utf-8")
            target.write_bytes(content)
    print(f"Exported bundle to {BUNDLE_DIR}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8089")
    parser.add_argument("--env", default=str(Path(__file__).resolve().parents[1] / ".env"))
    parser.add_argument("--no-export", action="store_true")
    args = parser.parse_args()

    env = read_env(Path(args.env))
    api = Superset(args.url, env.get("SUPERSET_ADMIN_USERNAME", "admin"), env["SUPERSET_ADMIN_PASSWORD"])
    database_id = ensure_database(api, env)
    datasets = {name: ensure_dataset(api, database_id, schema, table, verbose)
                for name, (schema, table, verbose) in DATASETS.items()}
    print("Datasets:", datasets)

    # Replace a previous version of the dashboard (and its charts).
    for dashboard in api.list_all("dashboard"):
        if dashboard.get("slug") == DASHBOARD_SLUG:
            old_charts = api.get(f"/api/v1/dashboard/{dashboard['id']}/charts")["result"]
            api.delete(f"/api/v1/dashboard/{dashboard['id']}")
            for chart in old_charts:
                api.delete(f"/api/v1/chart/{chart['id']}")

    dashboard_id = api.post("/api/v1/dashboard/", {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG,
                                                   "published": True})["id"]
    created = {}
    for chart in build_charts():
        dataset_id = datasets[chart["dataset"]]
        params = {"datasource": f"{dataset_id}__table", "viz_type": chart["viz_type"], "adhoc_filters": [],
                  "time_range": "No filter", **chart["params"]}
        result = api.post("/api/v1/chart/", {
            "slice_name": chart["name"], "viz_type": chart["viz_type"], "datasource_id": dataset_id,
            "datasource_type": "table", "params": json.dumps(params), "description": chart.get("description"),
            "dashboards": [dashboard_id]})
        chart_uuid = api.get(f"/api/v1/chart/{result['id']}")["result"].get("uuid") or str(uuid.uuid4())
        created[chart["key"]] = (result["id"], chart_uuid, chart["name"])
        print(f"Chart {result['id']}: {chart['name']}")

    metadata = {
        "color_scheme": "supersetColors",
        "label_colors": {**TEMPERATURE_BAND_COLORS, **LABEL_COLORS},
        "refresh_frequency": 3600,
        "timed_refresh_immune_slices": [],
        "cross_filters_enabled": True,
        "expanded_slices": {},
        "native_filter_configuration": native_filters(datasets, created),
        "chart_configuration": {},
    }
    api.put(f"/api/v1/dashboard/{dashboard_id}", {
        "position_json": json.dumps(build_layout(created)), "json_metadata": json.dumps(metadata),
        "css": "", "published": True})
    print(f"Dashboard {dashboard_id}: {args.url}/superset/dashboard/{DASHBOARD_SLUG}/")

    if not args.no_export:
        export_bundle(api, dashboard_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
