# airflow — Orchestration Airflow

Pipelines d'ingestion des données DVF vers Snowflake, basés sur **Astronomer Runtime**.

## Prérequis

- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- Docker Desktop
- Credentials AWS (S3) et Snowflake configurés

## Démarrage local

```bash
cd airflow/
astro dev start
```

L'UI Airflow est disponible sur **http://localhost:8080**
- Login : `admin` / `admin`

Pour arrêter :

```bash
astro dev stop
```

## DAGs

### `produce_data_api` — DAG principal

Télécharge les fichiers DVF depuis data.gouv.fr et les uploade vers S3.

**Flux :**
```
data.gouv.fr API
    │  GET /datasets/demandes-de-valeurs-foncieres/
    ▼
Parsing des URLs de téléchargement (BeautifulSoup)
    ▼
Téléchargement des fichiers .txt (pipe-delimited)
    ▼
AWS S3 : s3://data-platform-project-kubctl-1/real-raw/
```

**Caractéristiques :**
- Asset-based orchestration (Airflow SDK)
- Vérification d'existence avant upload (pas de doublons)
- Logging structuré à chaque étape
- Support multi-années (2020 → 2025)

---

### `data_produce` — Ingestion locale (développement)

Télécharge les fichiers DVF directement dans le répertoire local `real_estate_analytics/raw/`. Utile pour tester dbt sans passer par S3/Snowflake.

---

### `exampledag` — Exemple Astronomer

DAG de démonstration (API Open Notify / astronautes dans l'espace). Peut être supprimé en production.

## Connexions requises

À configurer dans l'UI Airflow ou dans `airflow_settings.yaml` :

| Conn ID | Type | Usage |
|---|---|---|
| `aws_default` | Amazon Web Services | Upload S3 |
| `snowflake_default` | Snowflake | Chargement COPY INTO |

## Dépendances Python

```
requests          # Appels HTTP data.gouv.fr
beautifulsoup4    # Parsing HTML pour extraction des URLs
pandas            # Manipulation de données
pyarrow           # Format Parquet
apache-airflow-providers-snowflake
apache-airflow-providers-amazon
```

## Structure

```
airflow/
├── dags/
│   ├── produce_data_api.py   # DAG principal (S3)
│   ├── data_produce.py       # Ingestion locale
│   └── exampledag.py         # Exemple Astronomer
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── Dockerfile                # Astro Runtime 3.1-11
├── requirements.txt
├── airflow_settings.yaml     # Connexions locales
└── packages.txt              # Dépendances OS
```

## Lancer les tests

```bash
cd airflow/
astro dev pytest
```
