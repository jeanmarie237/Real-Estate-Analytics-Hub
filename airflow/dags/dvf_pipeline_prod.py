"""
DVF Production Pipeline — v1.0
================================
Orchestration complète de la chaîne de données DVF :

    data.gouv.fr API
        → S3 (raw)
            → Snowflake DEV_BRONZE (COPY INTO)
                → dbt Silver  (déduplication, validation)
                    → dbt Gold  (agrégations métier)
                        → dbt Star Schema (Power BI)
                            → dbt test (qualité)

Fréquence : 5e jour du mois à 02h00 UTC
            (les fichiers DVF sont mis à jour le 1er du mois)

Architecture Airflow :
    - TaskGroup `ingestion`       : API → S3 (idempotent, skip si déjà présent)
    - TaskGroup `loading`         : S3 → Snowflake Bronze (idempotent COPY INTO)
    - TaskGroup `transformation`  : dbt seed → staging → silver → gold → star_schema
    - TaskGroup `quality`         : dbt test sur tous les modèles

Connexions Airflow requises :
    - aws_conn      : AWS (S3) — type Amazon Web Services
    - snowflake_conn: Snowflake — type Snowflake

Variables d'environnement requises (pour dbt via profiles.yml) :
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    (configurable via Astronomer UI → Environment Variables)
"""

from __future__ import annotations

import logging
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

import requests
from airflow.sdk import DAG, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
BUCKET_NAME      = "data-platform-project-kubctl-1"
S3_PREFIX        = "real-raw/"
AWS_CONN_ID      = "aws_conn"
SNOWFLAKE_CONN   = "snowflake_conn"

DBT_VENV         = "/usr/local/airflow/dbt_venv/bin"
DBT_PROJECT_DIR  = "/usr/local/airflow/include/dbt/real_estate_analytics"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt"
DBT_LOG_PATH     = "/tmp/dbt_logs"   # Volume monté en lecture seule → logs dans /tmp

DVF_API_URL      = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres/"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _on_failure_callback(context: dict) -> None:
    """Log détaillé en cas d'échec — étendre avec Slack/email si nécessaire."""
    logger.error(
        "PIPELINE FAILED | dag=%s | task=%s | run_id=%s | log_url=%s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["run_id"],
        context["task_instance"].log_url,
    )


def _dbt_cmd(select: str, cmd: str = "run") -> str:
    """Génère une commande dbt avec les chemins de production."""
    return (
        f"{DBT_VENV}/dbt {cmd}"
        f" --project-dir {DBT_PROJECT_DIR}"
        f" --profiles-dir {DBT_PROFILES_DIR}"
        f" --log-path {DBT_LOG_PATH}"
        f" --target prod"
        f" --select {select}"
        f" --no-use-colors"
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────

default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id      = "dvf_production_pipeline",
    description = "Pipeline DVF complet : data.gouv.fr → S3 → Snowflake → dbt → Star Schema",
    schedule    = "0 2 5 * *",       # 5e du mois, 2h UTC
    start_date  = datetime(2025, 1, 1),
    catchup     = False,
    max_active_runs = 1,             # Pas d'exécutions concurrentes
    tags        = ["dvf", "production", "snowflake", "dbt"],
    default_args = default_args,
    doc_md      = __doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ═══════════════════════════════════════════════════════════════════════════
    # TaskGroup : INGESTION — data.gouv.fr API → S3
    # ═══════════════════════════════════════════════════════════════════════════

    @task_group(group_id="ingestion")
    def ingestion_group() -> None:

        @task(task_id="fetch_dvf_to_s3", retries=3, retry_delay=timedelta(minutes=10))
        def fetch_dvf_to_s3() -> dict:
            """
            Télécharge les fichiers DVF depuis l'API data.gouv.fr et les uploade sur S3.
            Idempotent : vérifie S3 AVANT de télécharger chaque ZIP (évite ~25 min
            de download inutile quand les fichiers sont déjà présents).
            Pattern DVF : valeursfoncieres-YYYY.zip → valeursfoncieres-YYYY.txt
            """
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

            response = requests.get(DVF_API_URL, timeout=30)
            response.raise_for_status()
            dataset = response.json()

            zip_urls = [
                r["url"]
                for r in dataset["resources"]
                if r["url"].endswith(".zip") and "valeursfoncieres" in r["url"]
            ]
            logger.info("%d fichiers ZIP DVF détectés sur data.gouv.fr", len(zip_urls))

            # Liste les clés S3 existantes UNE SEULE FOIS — évite N*check_for_key
            existing_keys = set(s3.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX) or [])
            # Index lowercase pour comparaison insensible à la casse
            # (ZIPs DVF : nom lowercase, mais .txt internes en PascalCase sur S3)
            existing_keys_lower = {k.lower() for k in existing_keys}
            logger.info("%d fichiers déjà présents sur S3 (préfixe %s)", len(existing_keys), S3_PREFIX)

            uploaded, skipped = [], []

            for url in zip_urls:
                zip_name = url.split("/")[-1]

                # Vérifie S3 AVANT de télécharger le ZIP
                # Les ZIP DVF ont une double extension : valeursfoncieres-YYYY.txt.zip
                # → retirer seulement '.zip' donne valeursfoncieres-YYYY.txt
                # → comparaison lowercase car les .txt internes sont en PascalCase (ValeursFoncieres-...)
                expected_txt = zip_name[:-4]  # retire uniquement '.zip'
                expected_key = f"{S3_PREFIX}{expected_txt}"

                if expected_key.lower() in existing_keys_lower:
                    logger.info("SKIP ZIP (déjà sur S3) : %s", zip_name)
                    skipped.append(expected_txt)
                    continue

                logger.info("Téléchargement ZIP : %s", zip_name)
                r = requests.get(url, timeout=300)
                r.raise_for_status()

                with zipfile.ZipFile(BytesIO(r.content)) as z:
                    for file_name in z.namelist():
                        if not file_name.endswith(".txt"):
                            continue

                        s3_key = f"{S3_PREFIX}{file_name}"

                        if s3_key in existing_keys:
                            logger.info("SKIP (déjà présent sur S3) : %s", file_name)
                            skipped.append(file_name)
                            continue

                        logger.info("Upload S3 → %s", s3_key)
                        with z.open(file_name) as f:
                            s3.load_file_obj(
                                file_obj    = f,
                                key         = s3_key,
                                bucket_name = BUCKET_NAME,
                                replace     = False,
                            )
                        uploaded.append(file_name)
                        logger.info("Upload terminé : %s", file_name)

            summary = {
                "uploaded": uploaded,
                "skipped":  skipped,
                "total":    len(uploaded) + len(skipped),
            }
            logger.info("Ingestion terminée : %s", summary)
            return summary

        @task(task_id="validate_s3_upload")
        def validate_s3_upload(summary: dict) -> None:
            """Vérifie qu'au moins un fichier DVF est disponible sur S3."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
            keys = s3.list_keys(bucket_name=BUCKET_NAME, prefix=S3_PREFIX) or []
            dvf_files = [k for k in keys if k.endswith(".txt")]

            if not dvf_files:
                raise ValueError(
                    f"Aucun fichier DVF .txt trouvé sur S3 : s3://{BUCKET_NAME}/{S3_PREFIX}"
                )

            logger.info(
                "%d fichiers DVF disponibles sur S3 | ce run : +%d uploadés, %d skippés",
                len(dvf_files),
                len(summary.get("uploaded", [])),
                len(summary.get("skipped", [])),
            )

        summary = fetch_dvf_to_s3()
        validate_s3_upload(summary)

    # ═══════════════════════════════════════════════════════════════════════════
    # TaskGroup : LOADING — S3 → Snowflake DEV_BRONZE
    # ═══════════════════════════════════════════════════════════════════════════

    @task_group(group_id="loading")
    def loading_group() -> None:

        @task(task_id="create_snowflake_objects")
        def create_snowflake_objects() -> None:
            """
            Crée les objets Snowflake nécessaires au pipeline (idempotent).
            Exécute : 01_create_snowflake_objects.sql
            """
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            sql = Path("/usr/local/airflow/include/sql/01_create_snowflake_objects.sql").read_text()
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
            hook.run(sql)
            logger.info("Objets Snowflake créés / vérifiés avec succès")

        @task(task_id="create_or_replace_stage")
        def create_or_replace_stage() -> None:
            """
            Crée ou remplace le Snowflake External Stage pointant vers S3.
            Les credentials AWS sont lus depuis la connexion Airflow (aws_conn)
            et jamais stockés en dur dans le code.
            """
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            # Récupération des credentials depuis la connexion Airflow aws_conn
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            creds = s3_hook.get_credentials()

            snow_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
            snow_hook.run(
                f"""
                CREATE OR REPLACE STAGE DVF_DB.DEV_BRONZE.dvf_s3_stage
                    URL = 's3://{BUCKET_NAME}/{S3_PREFIX}'
                    CREDENTIALS = (
                        AWS_KEY_ID     = '{creds.access_key}'
                        AWS_SECRET_KEY = '{creds.secret_key}'
                    )
                    FILE_FORMAT = DVF_DB.DEV_BRONZE.dvf_csv_format
                    COMMENT = 'Stage DVF — credentials gérés par Airflow aws_conn';
                """
            )
            logger.info(
                "Stage DVF créé : @DVF_DB.DEV_BRONZE.dvf_s3_stage → s3://%s/%s",
                BUCKET_NAME, S3_PREFIX,
            )

        @task(task_id="copy_into_bronze")
        def copy_into_bronze() -> None:
            """
            Charge les fichiers DVF depuis le stage S3 vers Snowflake DEV_BRONZE.
            Idempotent : FORCE=FALSE (Snowflake skip les fichiers déjà chargés
            grâce à son registre interne COPY_HISTORY).
            """
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            sql = Path("/usr/local/airflow/include/sql/02_copy_into_bronze.sql").read_text()
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
            hook.run(sql)
            logger.info("COPY INTO DEV_BRONZE.mutations_foncieres terminé")

        create_snowflake_objects() >> create_or_replace_stage() >> copy_into_bronze()

    # ═══════════════════════════════════════════════════════════════════════════
    # TaskGroup : TRANSFORMATION — dbt (couches Silver → Gold → Star Schema)
    # ═══════════════════════════════════════════════════════════════════════════

    @task_group(group_id="transformation")
    def transformation_group() -> None:

        # dbt deps : télécharge les packages dbt (dbt_utils, etc.)
        dbt_deps = BashOperator(
            task_id      = "dbt_deps",
            bash_command = (
                f"mkdir -p {DBT_LOG_PATH} && "
                f"{DBT_VENV}/dbt deps"
                f" --project-dir {DBT_PROJECT_DIR}"
                f" --profiles-dir {DBT_PROFILES_DIR}"
                f" --log-path {DBT_LOG_PATH}"
                f" --no-use-colors"
            ),
        )

        # dbt seed : charge les données de référence (ref_departements.csv)
        dbt_seed = BashOperator(
            task_id      = "dbt_seed",
            bash_command = (
                f"{DBT_VENV}/dbt seed"
                f" --project-dir {DBT_PROJECT_DIR}"
                f" --profiles-dir {DBT_PROFILES_DIR}"
                f" --log-path {DBT_LOG_PATH}"
                f" --target prod"
                f" --no-use-colors"
            ),
        )

        # dbt run staging : vue src_dvf (renommage colonnes)
        dbt_staging = BashOperator(
            task_id      = "dbt_staging",
            bash_command = _dbt_cmd("staging"),
        )

        # dbt run silver : déduplication + validation + enrichissement (~17M rows)
        # Incrémental : seules les nouvelles mutations sont traitées
        dbt_silver = BashOperator(
            task_id      = "dbt_silver",
            bash_command = _dbt_cmd("silver"),
            execution_timeout = timedelta(hours=2),  # Silver peut être long (17M rows)
        )

        # dbt run gold : 9 tables d'agrégations métier
        dbt_gold = BashOperator(
            task_id      = "dbt_gold",
            bash_command = _dbt_cmd("gold"),
        )

        # dbt run star_schema : 4 dims + 1 fact + 4 AGG tables (Power BI)
        dbt_star_schema = BashOperator(
            task_id      = "dbt_star_schema",
            bash_command = _dbt_cmd("star_schema"),
            execution_timeout = timedelta(hours=1),
        )

        dbt_deps >> dbt_seed >> dbt_staging >> dbt_silver >> dbt_gold >> dbt_star_schema

    # ═══════════════════════════════════════════════════════════════════════════
    # TaskGroup : QUALITY — dbt test
    # ═══════════════════════════════════════════════════════════════════════════

    @task_group(group_id="quality")
    def quality_group() -> None:

        # dbt test : tous les tests (unique, not_null, relationships, custom)
        dbt_test = BashOperator(
            task_id      = "dbt_test",
            bash_command = (
                f"{DBT_VENV}/dbt test"
                f" --project-dir {DBT_PROJECT_DIR}"
                f" --profiles-dir {DBT_PROFILES_DIR}"
                f" --log-path {DBT_LOG_PATH}"
                f" --target prod"
                f" --no-use-colors"
            ),
        )

        # Résumé du run (logs des résultats de qualité)
        @task(task_id="log_quality_summary")
        def log_quality_summary() -> None:
            """Log le résumé des tests dbt depuis le fichier run_results.json."""
            import json
            results_path = Path(f"{DBT_PROJECT_DIR}/target/run_results.json")
            if not results_path.exists():
                logger.warning("run_results.json introuvable, skip résumé qualité")
                return

            results = json.loads(results_path.read_text())
            summary = results.get("results", [])
            passed  = sum(1 for r in summary if r.get("status") == "pass")
            warned  = sum(1 for r in summary if r.get("status") == "warn")
            failed  = sum(1 for r in summary if r.get("status") == "fail")
            logger.info(
                "dbt test summary → PASS=%d | WARN=%d | FAIL=%d | TOTAL=%d",
                passed, warned, failed, len(summary),
            )

        dbt_test >> log_quality_summary()

    # ─── Dépendances globales ─────────────────────────────────────────────────
    ing      = ingestion_group()
    loading  = loading_group()
    transform = transformation_group()
    quality  = quality_group()

    start >> ing >> loading >> transform >> quality >> end
