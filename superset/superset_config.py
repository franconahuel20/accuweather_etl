"""Superset configuration for the AccuWeather ETL stack (loaded from /app/pythonpath)."""
import os
from urllib.parse import quote

from superset.config import TALISMAN_CONFIG as DEFAULT_TALISMAN_CONFIG

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata (dashboards, charts, users) in PostgreSQL, on the persistent postgres_data volume.
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{user}:{password}@{host}:5432/{db}".format(
    user=quote(os.environ["SUPERSET_PG_USER"], safe=""),
    password=quote(os.environ["SUPERSET_PG_PASSWORD"], safe=""),
    host=os.environ.get("SUPERSET_PG_HOST", "postgres"),
    db=os.environ.get("SUPERSET_PG_DB", "superset"),
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Charts always query the warehouse (no result cache), so dashboards show the latest daily load.
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}

ROW_LIMIT = 50000
BABEL_DEFAULT_LOCALE = "en"

# Base maps for the deck.gl charts that work without a Mapbox token (Esri tiles use {z}/{y}/{x}).
DECKGL_BASE_MAP = [
    [
        "tile://https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        "Light gray (Esri)",
    ],
    ["tile://https://tile.openstreetmap.org/{z}/{x}/{y}.png", "Streets (OpenStreetMap)"],
    [
        "tile://https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        "Dark gray (Esri)",
    ],
    [
        "tile://https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "Satellite (Esri)",
    ],
]
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# Allow the map tile servers in the Content Security Policy.
_TILE_HOSTS = ["https://server.arcgisonline.com", "https://tile.openstreetmap.org"]
TALISMAN_CONFIG = {
    **DEFAULT_TALISMAN_CONFIG,
    "content_security_policy": {
        **DEFAULT_TALISMAN_CONFIG["content_security_policy"],
        "img-src": [*DEFAULT_TALISMAN_CONFIG["content_security_policy"]["img-src"], *_TILE_HOSTS],
        "connect-src": [*DEFAULT_TALISMAN_CONFIG["content_security_policy"]["connect-src"], *_TILE_HOSTS],
    },
}
