"""Registers the weather warehouse in Superset and imports the dashboard bundle.

* The bundle (superset/dashboards/<bundle>/...) is a Superset export: database, datasets, charts and
  dashboards as YAML. It is imported only when its dashboard does not exist yet, so changes made in
  the UI survive restarts. SUPERSET_FORCE_REIMPORT=true overwrites them with the bundled version.
* The database connection is refreshed on every start from the environment (.env), so rotating the
  read-only password never breaks the dashboards.
"""
import os
import sys
import uuid
from pathlib import Path
from urllib.parse import quote

import yaml

BUNDLES_DIR = Path(__file__).resolve().parent / "dashboards"


def warehouse_uri() -> str:
    return "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
        user=quote(os.environ["BI_READER_USER"], safe=""),
        password=quote(os.environ["BI_READER_PASSWORD"], safe=""),
        host=os.environ.get("WAREHOUSE_HOST", "postgres"),
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        db=os.environ.get("WAREHOUSE_DB", "weather"),
    )


def read_bundle(bundle: Path) -> dict[str, str]:
    """Returns {relative path: content} like superset's get_contents_from_bundle()."""
    contents = {}
    for path in sorted(bundle.rglob("*.yaml")):
        relative = path.relative_to(bundle).as_posix()
        text = path.read_text(encoding="utf-8")
        if relative.startswith("databases/"):
            config = yaml.safe_load(text)
            config["sqlalchemy_uri"] = warehouse_uri()
            text = yaml.safe_dump(config, sort_keys=False, allow_unicode=True)
        contents[relative] = text
    return contents


def main() -> int:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.utils.core import override_user

        admin = security_manager.find_user(username=os.environ.get("SUPERSET_ADMIN_USERNAME", "admin"))
        force = os.environ.get("SUPERSET_FORCE_REIMPORT", "false").lower() == "true"
        bundles = [path for path in sorted(BUNDLES_DIR.iterdir()) if (path / "metadata.yaml").exists()]
        if not bundles:
            print("No dashboard bundle found; nothing to import.")

        for bundle in bundles:
            contents = read_bundle(bundle)
            slugs = [
                yaml.safe_load(text).get("slug")
                for name, text in contents.items()
                if name.startswith("dashboards/")
            ]
            existing = db.session.query(Dashboard).filter(Dashboard.slug.in_([s for s in slugs if s])).count()
            if existing and not force:
                print(f"Dashboards {slugs} already exist: keeping the current version (SUPERSET_FORCE_REIMPORT=false).")
                continue
            with override_user(admin):
                ImportDashboardsCommand(contents, overwrite=True).run()
            print(f"Imported dashboard bundle '{bundle.name}' ({len(contents)} files): {slugs}")

        # Keep the warehouse connection in sync with .env.
        for bundle in bundles:
            for name, text in read_bundle(bundle).items():
                if not name.startswith("databases/"):
                    continue
                config = yaml.safe_load(text)
                database = db.session.query(Database).filter_by(uuid=uuid.UUID(str(config["uuid"]))).one_or_none()
                if database is not None:
                    database.set_sqlalchemy_uri(warehouse_uri())
                    db.session.commit()
                    print(f"Database '{database.database_name}' connection refreshed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
