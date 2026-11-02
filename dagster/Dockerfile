FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DBT_PROJECT_DIR=/opt/dagster/app/dbt \
    DBT_PROFILES_DIR=/opt/dagster/app/dbt \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    PYTHONPATH=/opt/dagster/app

COPY dagster/requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt

RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin dagster \
    && mkdir -p /opt/dagster/app "${DAGSTER_HOME}/storage" /opt/accuweather/historical

WORKDIR /opt/dagster/app
COPY dagster/dagster.yaml dagster/workspace.yaml ${DAGSTER_HOME}/
COPY dagster/accuweather_pipeline ./accuweather_pipeline
COPY dbt ./dbt
RUN chown -R dagster:dagster /opt/dagster

USER dagster
# Ship the dbt manifest inside the image and fail the build if the code location cannot load.
RUN dbt parse --project-dir dbt --profiles-dir dbt --quiet \
    && python -c "import accuweather_pipeline.definitions"

EXPOSE 3000 4000
