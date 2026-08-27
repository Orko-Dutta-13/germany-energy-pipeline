FROM apache/airflow:2.9.2

USER airflow

# Pin dbt strictly to stable 1.x — exclude 2.0 beta (dbt Fusion) which
# dropped Postgres support. "<2.0" ensures we never accidentally pick it up.
RUN pip install --no-cache-dir "dbt-postgres>=1.7,<2.0"

# Install remaining packages after dbt has settled its deps
RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    "prophet>=1.1" \
    "scikit-learn>=1.0"
