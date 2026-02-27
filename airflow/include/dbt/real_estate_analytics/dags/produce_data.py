# # import requests
# # from bs4 import BeautifulSoup
# # from io import BytesIO
# # import zipfile
# # import os

# # # Dossier où tu stockes les fichiers extraits
# # os.makedirs("raw", exist_ok=True)

# # # URL de la page des DVF
# # url = "https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/"

# # # Récupérer le HTML
# # response = requests.get(url)
# # soup = BeautifulSoup(response.content, "html.parser")

# # # Rechercher tous les liens ZIP
# # zip_links = []
# # for a in soup.find_all("a", href=True):
# #     if a["href"].endswith(".zip"):
# #         zip_links.append(a["href"])

# # print(f"{len(zip_links)} fichiers ZIP trouvés.")

# # # Télécharger et extraire chaque ZIP
# # for link in zip_links:
# #     # URL complète si besoin
# #     full_url = link
# #     if not link.startswith("http"):
# #         full_url = "https://www.data.gouv.fr" + link
    
# #     print(f"Téléchargement : {full_url}")
# #     r = requests.get(full_url)
    
# #     if r.status_code == 200:
# #         with zipfile.ZipFile(BytesIO(r.content)) as z:
# #             for file_name in z.namelist():
# #                 if file_name.endswith(".zip"):
# #                     print(f"Extraction du fichier : {file_name}")
# #                     z.extract(file_name, path="raw")
# #     else:
# #         print(f"Erreur téléchargement : {r.status_code}")


# import requests
# import zipfile
# from io import BytesIO
# import os

# RAW_DIR = "raw"
# os.makedirs(RAW_DIR, exist_ok=True)

# DATASET_API = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres/"

# response = requests.get(DATASET_API)
# response.raise_for_status()

# dataset = response.json()
# resources = dataset["resources"]

# zip_files = [
#     r for r in resources
#     if r["url"].endswith(".zip") and "valeursfoncieres" in r["url"]
# ]

# print(f"{len(zip_files)} fichiers ZIP DVF détectés")

# for r in zip_files:
#     url = r["url"]
#     filename = url.split("/")[-1]

#     print(f"Téléchargement : {filename}")

#     resp = requests.get(url)
#     resp.raise_for_status()

#     with zipfile.ZipFile(BytesIO(resp.content)) as z:
#         z.extractall(RAW_DIR)

# print("✅ Extraction DVF terminée")

import requests
import zipfile
from io import BytesIO
import os
import re

RAW_DIR = "raw"
os.makedirs(RAW_DIR, exist_ok=True)

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres/"

response = requests.get(DATASET_API)
response.raise_for_status()

dataset = response.json()
resources = dataset["resources"]

zip_files = [
    r for r in resources
    if r["url"].endswith(".zip") and "valeursfoncieres" in r["url"]
]

print(f"{len(zip_files)} fichiers DVF détectés")

for r in zip_files:
    url = r["url"]
    zip_name = url.split("/")[-1]
    year_match = re.search(r"20\d{2}", zip_name)

    # nom du fichier txt attendu après extraction
    extracted_files = [
        f for f in os.listdir(RAW_DIR)
        if year_match and year_match.group() in f
    ]

    if extracted_files:
        print(f"⏭️  {zip_name} déjà présent → SKIP")
        continue

    print(f"⬇️  Téléchargement : {zip_name}")
    resp = requests.get(url)
    resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(resp.content)) as z:
        z.extractall(RAW_DIR)

    print(f"✅ {zip_name} extrait")

print("🎯 Ingestion DVF terminée")

