# Real Estate Analytics Hub

> **End-to-end production data pipeline** on 17M+ French real estate transactions — from government open data to Power BI analytics, fully automated with Apache Airflow on Astronomer.

[![CI](https://github.com/YOUR_USERNAME/Real-Estate-Analytics-Hub/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/Real-Estate-Analytics-Hub/actions/workflows/ci.yml)
[![dbt](https://img.shields.io/badge/dbt-1.8.4-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud_DWH-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-3.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Astronomer](https://img.shields.io/badge/Astronomer-Runtime_3.1-blueviolet)](https://www.astronomer.io/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/s3/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

## Overview

This project implements a **production-grade ELT pipeline** to ingest, transform, and deliver analytics on the French *Demandes de Valeurs Foncières* (DVF) dataset — every real estate transaction recorded in France from 2020 to 2025, published by the DGFiP.

The architecture follows the **Medallion pattern** (Bronze → Silver → Gold) combined with a **Star Schema** layer purpose-built for Power BI, orchestrated end-to-end by Apache Airflow running on Astronomer Cloud.

### Key Figures

| Metric | Value |
|---|---|
| Transactions processed | **17+ million** |
| Coverage | France nationwide, 2020–2025 |
| Communes indexed | **36,597** |
| Departments | **101** |
| dbt models | **20** (staging → silver → gold → star schema) |
| dbt tests | **PASS ✅ / WARN expected / ERROR 0** |
| Power BI footprint | **< 50 MB** (pre-aggregated in Snowflake) |
| Incremental refresh | **< 90 seconds** |
| Ingestion (subsequent runs) | **< 5 seconds** (S3 idempotency check before download) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                                      │
│   data.gouv.fr  ──── DVF Open Data (pipe-delimited CSV, semi-annual)    │
│                       DGFiP · Licence Ouverte 2.0                       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  REST API + ZIP download
                    ┌──────────▼──────────┐
                    │   Apache Airflow    │  Orchestration
                    │ (Astronomer 3.1)    │  DAG: dvf_production_pipeline
                    │                     │  Schedule: 5th of month, 2h UTC
                    └──────────┬──────────┘
                               │  Idempotent: check S3 before download
                    ┌──────────▼──────────┐
                    │      AWS S3         │  Raw staging area
                    │  s3://dvf-raw/      │  ValeursFoncieres-YYYY.txt
                    └──────────┬──────────┘
                               │  COPY INTO via Snowflake External Stage
┌──────────────────────────────▼──────────────────────────────────────────┐
│                    Snowflake  ·  DVF_DB                                 │
│                                                                         │
│  DEV_BRONZE  (raw load)                                                 │
│  ├── mutations_foncieres      TABLE  ← COPY INTO (MATCH_BY_COLUMN_NAME) │
│  └── src_dvf                  VIEW   ← Column renaming only             │
│                                                                         │
│  DEV_SILVER  (cleaned, deduped)      [dbt incremental]                  │
│  └── silver_mutation_f        TABLE  ← Casting, validation, MD5 key     │
│                               ~17M rows · unique mutation_id            │
│                                                                         │
│  DEV_GOLD  (business aggregations)   [dbt tables]                       │
│  ├── gold_kpis                ← 1-row France-wide KPIs                  │
│  ├── evolution_annuelle       ← YoY trends with LAG()                   │
│  ├── prix_moyen_commune       ← Avg price per commune × type            │
│  ├── prix_m2_commune          ← Price/m² per commune × type             │
│  ├── prix_par_departement     ← Department-level benchmarks             │
│  ├── volume_mensuel           ← Monthly time series                     │
│  ├── repartition_types        ← Property type distribution              │
│  ├── surface_vs_prix          ← Price by surface bins                   │
│  └── top_communes             ← Top 50 (min 50 transactions)            │
│                                                                         │
│  DEV_STAR  (Power BI layer)          [dbt tables — dimensional model]   │
│  ├── dim_date                 ← 2020–2026, full calendar attributes     │
│  ├── dim_geography            ← 36k communes + dept + region            │
│  ├── dim_type_bien            ← 5 property types (categorized)          │
│  ├── dim_nature_mutation      ← 6 transaction types                     │
│  ├── fact_mutations           ← 17M rows (grain: 1 transaction)         │
│  ├── agg_annuel_type_bien     ← ~30 rows (KPI cards, YoY)               │
│  ├── agg_mensuel_type_bien    ← ~3k rows (time series charts)           │
│  ├── agg_departement_type_bien← ~3k rows (choropleth map)               │
│  └── agg_commune_type_bien    ← ~150k rows (commune benchmark)          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  Import mode (< 50 MB, < 90s refresh)
                    ┌──────────▼──────────┐
                    │     Power BI        │   Analytics dashboards
                    │  Composite model    │  
                    └─────────────────────┘
```

---

## Technology Stack

| Tool | Version | Role |
|---|---|---|
| **Apache Airflow** | Astro Runtime 3.1 | Pipeline orchestration, scheduling |
| **Astronomer** | Cloud / local CLI | Managed Airflow deployment |
| **dbt-snowflake** | 1.8.4 | ELT transformations, data modeling, testing |
| **Snowflake** | — | Cloud data warehouse (DVF_DB, DVF_WH) |
| **AWS S3** | — | Intermediate raw file storage |
| **Python** | 3.12 | Airflow DAGs, ingestion logic |
| **Power BI Desktop** | Latest | Analytics dashboards |
| **dbt-utils** | 1.3.3 | Surrogate key generation |
| **GitHub Actions** | — | CI: DAG syntax, dbt compile, Docker build |

---

## Airflow Pipeline — TaskGroups

The production DAG (`dvf_production_pipeline`) runs on the **5th of each month at 02:00 UTC**, with `max_active_runs=1` to prevent concurrent executions.

```
start
  └── ingestion
  │     ├── fetch_dvf_to_s3       ← API → ZIP → S3 (skip if already present)
  │     └── validate_s3_upload    ← Assert DVF files exist on S3
  └── loading
  │     ├── create_snowflake_objects  ← Idempotent DDL (IF NOT EXISTS)
  │     ├── create_or_replace_stage   ← External Stage S3 (AWS creds via Airflow conn)
  │     └── copy_into_bronze          ← COPY INTO with MATCH_BY_COLUMN_NAME
  └── transformation
  │     ├── dbt_deps      ← Install dbt packages
  │     ├── dbt_seed      ← Load ref_departements.csv
  │     ├── dbt_staging   ← src_dvf view
  │     ├── dbt_silver    ← Incremental cleaning + dedup (~52s on 17M rows)
  │     ├── dbt_gold      ← 9 business aggregation tables
  │     └── dbt_star_schema ← 4 dims + 1 fact + 4 AGG tables
  └── quality
        ├── dbt_test              ← All dbt tests (unique, not_null, custom)
        └── log_quality_summary   ← PASS/WARN/FAIL summary in logs
end
```

**Idempotency at every stage:**
- S3 ingestion: checks existing keys before downloading ZIPs (< 5s on re-runs)
- Snowflake DDL: `IF NOT EXISTS` on all objects
- COPY INTO: `FORCE=FALSE` (Snowflake internal COPY_HISTORY registry)
- Silver: `dbt incremental` with `unique_key='mutation_id'` → MERGE semantics

---

## Project Highlights

### 1. Incremental Processing at Scale

The Silver layer uses dbt's **incremental materialization** to process only new records, avoiding a full 17M-row reload on each monthly refresh:

```sql
-- silver_mutation_f.sql
{{ config(materialized='incremental', unique_key='mutation_id') }}

{% if is_incremental() %}
WHERE date_mutation > (SELECT MAX(date_mutation) FROM {{ this }})
{% endif %}
```

The `mutation_id` (MD5 over 11 composite fields) ensures the `MERGE` on Snowflake never creates duplicates even if a record is re-ingested.

### 2. Robust Deduplication

DVF transactions can include multiple lots per `no_disposition`. Without deduplication, price averages are inflated. The fix uses `ROW_NUMBER()` over the 4-field lot signature:

```sql
ROW_NUMBER() OVER (
    PARTITION BY
        no_disposition,
        identifiant_document,
        COALESCE(type_local, ''),
        COALESCE(surface_reelle_bati::STRING, '')
    ORDER BY no_disposition
) AS lot_rank
-- Keep only lot_rank = 1
```

### 3. Differentiated Data Quality Rules

Silent bad data is worse than a pipeline failure. Business validation rules are enforced in the Silver model, by property type:

```sql
CASE
    WHEN type_local IN ('maison', 'appartement')
        THEN valeur_fonciere >= 1000
            AND (surface_reelle_bati >= 9 OR surface_reelle_bati IS NULL)
    WHEN type_local = 'dépendance'
        THEN valeur_fonciere >= 500
    WHEN type_local = 'local industriel. commercial ou assimilé'
        THEN valeur_fonciere >= 1000
            AND (surface_reelle_bati >= 5 OR surface_reelle_bati IS NULL)
    ELSE valeur_fonciere >= 1000
END
```

Legitimate NULLs (donations, exchanges, land without buildings) are documented in dbt schema YAML and tracked as expected `WARN` — not `ERROR`.

### 4. Power BI Optimization — Pre-aggregated Star Schema

17 million rows in Power BI Import mode = ~4 GB RAM. The solution: push aggregation into Snowflake via dbt, expose only pre-computed tables:

| AGG Table | Rows | Usage |
|---|---|---|
| `agg_annuel_type_bien` | ~30 | KPI cards, YoY variation |
| `agg_mensuel_type_bien` | ~3,000 | Time series charts |
| `agg_departement_type_bien` | ~3,000 | Choropleth map |
| `agg_commune_type_bien` | ~150,000 | Commune benchmark, scatter |

**Total import: < 50 MB. Refresh: < 90 seconds. Visual response: < 500ms.**

YoY variations pre-computed in SQL, eliminating complex DAX:

```sql
ROUND(
    (prix_moyen - LAG(prix_moyen) OVER (PARTITION BY type_bien_key ORDER BY annee))
    / NULLIF(LAG(prix_moyen) OVER (PARTITION BY type_bien_key ORDER BY annee), 0) * 100,
    2
) AS variation_prix_yoy_pct
```

### 5. S3 Idempotency — Check Before Download

DVF ZIPs are 200–800 MB each. The naive approach (download → extract → check S3) wastes 25 minutes on re-runs. The production approach: list S3 keys once, compare before any download.

The key insight: DVF ZIP names use a double extension (`valeursfoncieres-2024.txt.zip`) and internal filenames use PascalCase (`ValeursFoncieres-2024.txt`). Matching requires stripping only `.zip` and doing a case-insensitive lookup:

```python
existing_keys_lower = {k.lower() for k in existing_keys}
expected_txt = zip_name[:-4]          # strips only '.zip'
if f"{S3_PREFIX}{expected_txt}".lower() in existing_keys_lower:
    logger.info("SKIP ZIP: %s", zip_name)
    continue
```

Result: subsequent runs complete ingestion in **< 5 seconds**.

---

## Project Structure

```
Real-Estate-Analytics-Hub/
│
├── airflow/                              # Astronomer project (Docker build context)
│   ├── dags/
│   │   └── dvf_pipeline_prod.py         # Production DAG — 4 TaskGroups, 15 tasks
│   │
│   ├── include/
│   │   ├── dbt/
│   │   │   ├── profiles.yml             # dbt Snowflake connection (env vars)
│   │   │   └── real_estate_analytics/   # dbt project
│   │   │       ├── dbt_project.yml
│   │   │       ├── packages.yml         # dbt-utils 1.3.3
│   │   │       ├── seeds/
│   │   │       │   └── ref_departements.csv  # 95 depts + region + zone analytique
│   │   │       └── models/
│   │   │           ├── staging/         # src_dvf (view, column renaming)
│   │   │           ├── silver/          # silver_mutation_f (incremental, ~17M rows)
│   │   │           ├── gold/            # 9 business aggregation tables
│   │   │           └── star_schema/     # 4 dims + 1 fact + 4 AGG tables
│   │   │
│   │   └── sql/
│   │       ├── 01_create_snowflake_objects.sql  # Idempotent DDL
│   │       └── 02_copy_into_bronze.sql          # COPY INTO (MATCH_BY_COLUMN_NAME)
│   │
│   ├── Dockerfile                        # Astro Runtime 3.1 + dbt-snowflake venv
│   ├── requirements.txt                  # Python dependencies
│   ├── airflow_settings.yaml             # Connections managed via CLI
│   └── .env                             # Local env vars (SNOWFLAKE_*, AWS_*)
│
├── .github/
│   └── workflows/
│       └── ci.yml                        # 4 jobs: DAG syntax, dbt compile, tests, Docker
│
├── docs/
│   ├── ARTICLE_MEDIUM_EN.md             # Medium article (English)
│   └── ARTICLE_MEDIUM_FR.md             # Medium article (French)
│
├── POWERBI_DESIGN.md                    # Full Power BI specification (DAX, layouts, theme)
└── README.md
```

---

## Data Model — Snowflake Schemas

### Medallion Architecture

```
BRONZE  (DEV_BRONZE)
├── mutations_foncieres   TABLE   — raw COPY INTO from S3 (42 VARCHAR columns)
└── src_dvf               VIEW    — column renaming only, no transformations

SILVER  (DEV_SILVER)
└── silver_mutation_f     INCREMENTAL TABLE
                          • Type casting (DATE, FLOAT, INT)
                          • Business validation by property type
                          • Lot deduplication via ROW_NUMBER + PARTITION
                          • MD5 unique key (mutation_id, 11 fields)
                          • Derived: prix_metre_carre

GOLD    (DEV_GOLD)         — 9 business aggregation tables
├── gold_kpis              TABLE  — 1 row, France-wide summary KPIs
├── evolution_annuelle     TABLE  — Annual trends + YoY via LAG()
├── prix_moyen_commune     TABLE  — Avg price by commune × property type
├── prix_m2_commune        TABLE  — Price/m² by commune × type
├── prix_par_departement   TABLE  — Department-level benchmarks
├── volume_mensuel         TABLE  — Monthly time series
├── repartition_types      TABLE  — Property type distribution
├── surface_vs_prix        TABLE  — Price by surface bucket × type
└── top_communes           TABLE  — Top 50 communes (min 50 transactions)

STAR    (DEV_STAR)         — Dimensional model for Power BI
├── dim_date               TABLE  — 2020–2026, full calendar attributes
├── dim_geography          TABLE  — 36,597 communes + dept + region + zone
├── dim_type_bien          TABLE  — 5 property types (categorized + sort order)
├── dim_nature_mutation    TABLE  — 6 transaction types
├── fact_mutations         TABLE  — 17M rows (grain: 1 transaction)
├── agg_annuel_type_bien   TABLE  — ~30 rows (year × property type)
├── agg_mensuel_type_bien  TABLE  — ~3k rows (month × type)
├── agg_departement_type_bien TABLE — ~3k rows (dept × type × year)
└── agg_commune_type_bien  TABLE  — ~150k rows (commune × type × year, min 5 txns)
```

---

## Data Quality

dbt tests run at the end of every pipeline execution (`quality.dbt_test` task).

| Test | Layer | Type | Notes |
|---|---|---|---|
| `mutation_id` UNIQUE | Silver | PASS | MD5 on 11 composite fields |
| `mutation_id` NOT NULL | Silver | PASS | — |
| `valeur_fonciere` NOT NULL | Silver | WARN (expected) | Donations/exchanges — no price |
| `prix_metre_carre` NOT NULL | Silver | WARN (expected) | Land-only transactions |
| `date_key` NOT NULL | Fact | PASS | — |
| `geo_key` NOT NULL | Fact | PASS | — |
| `type_bien_key` NOT NULL | Fact | PASS | — |
| `geo_key` UNIQUE | dim_geography | PASS | — |
| `date_key` UNIQUE | dim_date | PASS | — |
| `total_transactions` NOT NULL | gold_kpis | PASS | — |

---

## Installation & Local Development

### Prerequisites

- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Docker Desktop
- Snowflake account with `DVF_BI_ROLE` on `DVF_DB`
- AWS credentials (S3 read/write on the staging bucket)

### 1. Start Airflow locally

```bash
cd airflow/
astro dev start
# Airflow UI → http://localhost:8080 (admin/admin)
```

### 2. Add Airflow connections

```bash
# AWS S3
astro dev run connections add aws_conn \
  --conn-type aws \
  --conn-login YOUR_AWS_KEY_ID \
  --conn-password YOUR_AWS_SECRET_KEY

# Snowflake
astro dev run connections add snowflake_conn \
  --conn-type snowflake \
  --conn-host YOUR_ACCOUNT.snowflakecomputing.com \
  --conn-login YOUR_USER \
  --conn-password YOUR_PASSWORD \
  --conn-schema DEV_BRONZE \
  --conn-extra '{"account": "YOUR_ACCOUNT", "warehouse": "DVF_WH", "database": "DVF_DB", "role": "DVF_BI_ROLE"}'
```

### 3. Configure environment variables

Create `airflow/.env`:

```
SNOWFLAKE_ACCOUNT=YOUR_ACCOUNT          # e.g. ZPSXDDX-FD93437 (no .snowflakecomputing.com)
SNOWFLAKE_USER=YOUR_USER
SNOWFLAKE_PASSWORD=YOUR_PASSWORD
SNOWFLAKE_ROLE=DVF_BI_ROLE
SNOWFLAKE_DATABASE=DVF_DB
SNOWFLAKE_WAREHOUSE=DVF_WH
```

### 4. Fix dbt write permissions (first run only)

The dbt project is bind-mounted into the Docker container. The container user (`astro`, UID 50000) needs write access to create `dbt_packages/`:

```bash
chmod 777 airflow/include/dbt/real_estate_analytics/
```

### 5. Trigger the pipeline

```bash
astro dev run dags trigger dvf_production_pipeline
```

---

## Power BI Dashboard

Connect Power BI Desktop to Snowflake → `DVF_DB.DEV_STAR` in Import mode.

| Page | Primary Source | Key Visuals |
|---|---|---|
| **1. Vue Marché** | `AGG_ANNUEL` + `AGG_MENSUEL` | KPI cards, monthly trend, Top-N ranking |
| **2. Carte Géographique** | `AGG_DEPARTEMENT` | Choropleth map, regional benchmark |
| **3. Évolution du Marché** | `AGG_ANNUEL` + `AGG_MENSUEL` | Annual trends, YoY variation |
| **4. Benchmark Prix m²** | `AGG_COMMUNE` | Scatter plot, department ranking |
| **5. Profil Transactions** | `AGG_ANNUEL` + `AGG_COMMUNE` | Type distribution, surface bins |
| **6. Détail Département** | `AGG_DEPARTEMENT` | Drill-through page |
| **7. Fiche Commune** | `AGG_COMMUNE` | Drill-through page |



Full specification: [POWERBI_DESIGN.md](POWERBI_DESIGN.md)

---

## CI/CD

`.github/workflows/ci.yml` runs on every push and PR:

| Job | What It Does |
|---|---|
| `validate-dags` | Python AST syntax check on all DAG files |
| `dbt-compile` | `dbt compile` (SQL + Jinja validation, no DB connection) |
| `dbt-test` | `dbt test` against Snowflake (requires `SNOWFLAKE_TESTS_ENABLED=true`) |
| `validate-dockerfile` | `hadolint` lint + `docker build` smoke test |

---

## Data Source

- **Dataset:** [Demandes de Valeurs Foncières (DVF)](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/)
- **Publisher:** Direction Générale des Finances Publiques (DGFiP) 
- **License:** Licence Ouverte / Open Licence 2.0
- **Update frequency:** Semi-annual (S1 in July, S2 in January)
- **Coverage:** France nationwide including overseas territories

---

## Author

Built as a portfolio project demonstrating production-grade data engineering:
ingestion, ELT transformation, dimensional modeling, data quality, and analytics delivery.

*Stack: Apache Airflow (Astronomer) · dbt-Snowflake · AWS S3 · Power BI · Python 3.12*
