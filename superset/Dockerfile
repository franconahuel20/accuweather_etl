FROM apache/superset:6.0.0

USER root
# PostgreSQL driver for the metadata database and the weather warehouse (not shipped in the image).
RUN uv pip install --python /app/.venv/bin/python --no-cache psycopg2-binary==2.9.10

COPY superset/superset_config.py /app/pythonpath/superset_config.py
COPY superset/bootstrap.sh superset/import_assets.py /app/docker/accuweather/
COPY superset/dashboards /app/docker/accuweather/dashboards
# Strip Windows line endings in case the repository was checked out with CRLF.
RUN sed -i 's/\r$//' /app/docker/accuweather/bootstrap.sh \
    && chmod 0755 /app/docker/accuweather/bootstrap.sh \
    && chown -R superset:superset /app/docker/accuweather

USER superset
CMD ["/app/docker/accuweather/bootstrap.sh"]
