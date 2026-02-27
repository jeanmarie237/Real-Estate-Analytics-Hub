# from airflow.sdk import Asset, asset
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import requests
# import zipfile
# from io import BytesIO
# import logging

# # -------------------
# # LOGGING
# # -------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )
# logger = logging.getLogger(__name__)

# # -------------------
# # S3 CONFIG
# # -------------------
# BUCKET_NAME = "data-platform-project-kubctl-1"
# S3_PREFIX = "real-raw/"
# AWS_CONN_ID = "aws_conn"

# s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

# # -------------------
# # ASSET
# # -------------------
# dvf_raw_asset = Asset(
#     uri="s3://data-platform-project-kubctl-1/real-raw",
#     extra={
#         "source": "data.gouv.fr",
#         "layer": "raw",
#         "format": "txt"
#     }
# )

# DATASET_API = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres/"

# # -------------------
# # ASSET INGESTION
# # -------------------
# @asset(
#     name="dvf_raw_ingestion",
#     schedule="@daily",
#     description="Ingestion DVF RAW vers S3",
#     #outlets=[dvf_raw_asset],
# )
# def extract_dvf_to_s3():

#     logger.info("🚀 Démarrage ingestion DVF → S3")

#     response = requests.get(DATASET_API)
#     response.raise_for_status()
#     dataset = response.json()

#     zip_urls = [
#         r["url"]
#         for r in dataset["resources"]
#         if r["url"].endswith(".zip") and "valeursfoncieres" in r["url"]
#     ]

#     logger.info(f"{len(zip_urls)} fichiers ZIP détectés")

#     for url in zip_urls:
#         zip_name = url.split("/")[-1]
#         logger.info(f"📦 Traitement : {zip_name}")

#         r = requests.get(url)
#         r.raise_for_status()

#         with zipfile.ZipFile(BytesIO(r.content)) as z:
#             for file_name in z.namelist():

#                 if not file_name.endswith(".txt"):
#                     continue

#                 s3_key = f"{S3_PREFIX}{file_name}"

#                 if s3_hook.check_for_key(s3_key, BUCKET_NAME):
#                     logger.info(f"⏭️  {file_name} déjà présent → SKIP")
#                     continue

#                 logger.info(f"⬆️  Upload S3 : {s3_key}")

#                 with z.open(file_name) as f:
#                     s3_hook.load_file_obj(
#                         file_obj=f,
#                         key=s3_key,
#                         bucket_name=BUCKET_NAME,
#                         replace=False
#                     )

#                 logger.info(f"✅ Upload terminé : {file_name}")

#     logger.info("🎯 Ingestion DVF RAW terminée")
