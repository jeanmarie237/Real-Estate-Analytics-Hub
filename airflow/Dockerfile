FROM astrocrpublic.azurecr.io/runtime:3.1-11

# ─── dbt-snowflake dans un venv isolé ────────────────────────────────────────
# Isolement nécessaire pour éviter les conflits de dépendances avec Airflow.
# Version fixée pour reproductibilité en production.
# Le venv est disponible à /usr/local/airflow/dbt_venv/bin/dbt
RUN python -m venv /usr/local/airflow/dbt_venv \
 && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-snowflake==1.8.4 \
 && /usr/local/airflow/dbt_venv/bin/dbt --version
