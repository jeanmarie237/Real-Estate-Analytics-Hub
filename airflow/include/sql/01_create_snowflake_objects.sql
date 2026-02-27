-- ============================================================
-- Script : 01_create_snowflake_objects.sql
-- Description : Création idempotente des objets Snowflake nécessaires
--               au pipeline DVF (schemas, file format, table bronze).
-- Exécution  : Une seule fois au démarrage du pipeline (idempotent).
-- Prérequis  : Rôle DVF_BI_ROLE avec CREATE SCHEMA / TABLE sur DVF_DB.
-- ============================================================

USE DATABASE DVF_DB;
USE WAREHOUSE DVF_WH;
USE ROLE DVF_BI_ROLE;

-- ─── Schémas (couche Medallion) ──────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS DEV_BRONZE COMMENT = 'Bronze : données brutes DVF depuis S3';
CREATE SCHEMA IF NOT EXISTS DEV_SILVER COMMENT = 'Silver : données dédupliquées et validées';
CREATE SCHEMA IF NOT EXISTS DEV_GOLD   COMMENT = 'Gold : agrégations métier';
CREATE SCHEMA IF NOT EXISTS DEV_STAR   COMMENT = 'Star Schema : optimisé Power BI';

-- ─── File Format ─────────────────────────────────────────────────────────────
-- Fichiers DVF : pipe-separated, UTF-8, 1 ligne d'entête
CREATE FILE FORMAT IF NOT EXISTS DEV_BRONZE.dvf_csv_format
    TYPE                         = 'CSV'
    FIELD_DELIMITER              = '|'
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ENCODING                     = 'UTF8'
    TRIM_SPACE                   = TRUE
    EMPTY_FIELD_AS_NULL          = TRUE
    NULL_IF                      = ('', 'NULL', 'null')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Format fichiers DVF (data.gouv.fr) — pipe-separated UTF-8';

-- ─── Table Bronze : mutations_foncieres ──────────────────────────────────────
-- Couche RAW : noms de colonnes = headers exacts des fichiers DVF.
-- Tout en VARCHAR → aucune transformation en Bronze.
-- Les conversions de types se font en Silver (silver_mutation_f.sql).
--
-- Source : https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
-- Format : 42 colonnes pipe-séparées
CREATE TABLE IF NOT EXISTS DEV_BRONZE.mutations_foncieres (

    -- ── Colonnes DVF (42 colonnes, noms = headers CSV exacts) ─────────────────
    -- Compatible avec la table existante. Pas de colonnes metadata (_source_file
    -- etc.) pour ne pas casser les tables déjà créées. Le chargement utilise
    -- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE.
    "No disposition"                VARCHAR  COMMENT 'Numéro de disposition dans l''acte',
    "Identifiant de document"       VARCHAR  COMMENT 'Identifiant unique de l''acte notarié',
    "Reference document"            VARCHAR  COMMENT 'Référence du document',
    "1 Articles CGI"                VARCHAR  COMMENT 'Article CGI 1',
    "2 Articles CGI"                VARCHAR  COMMENT 'Article CGI 2',
    "3 Articles CGI"                VARCHAR  COMMENT 'Article CGI 3',
    "4 Articles CGI"                VARCHAR  COMMENT 'Article CGI 4',
    "5 Articles CGI"                VARCHAR  COMMENT 'Article CGI 5',
    "Motif de la non-price"         VARCHAR  COMMENT 'Motif absence de prix',
    "Nature mutation"               VARCHAR  COMMENT 'Nature de la mutation (vente, donation...)',
    "Valeur fonciere"               VARCHAR  COMMENT 'Prix de cession en euros (virgule = séparateur décimal)',
    "No voie"                       VARCHAR  COMMENT 'Numéro de voie',
    "B/T/Q"                         VARCHAR  COMMENT 'Indice de répétition voie',
    "Type de voie"                  VARCHAR  COMMENT 'Type de voie (rue, avenue, etc.)',
    "Code voie"                     VARCHAR  COMMENT 'Code RIVOLI de la voie',
    "Voie"                          VARCHAR  COMMENT 'Nom de la voie',
    "Code postal"                   VARCHAR  COMMENT 'Code postal',
    "Commune"                       VARCHAR  COMMENT 'Nom de la commune',
    "Code departement"              VARCHAR  COMMENT 'Code département (2 caractères)',
    "Code commune"                  VARCHAR  COMMENT 'Code INSEE de la commune',
    "Section"                       VARCHAR  COMMENT 'Section cadastrale',
    "No plan"                       VARCHAR  COMMENT 'Numéro de plan cadastral',
    "No Volume"                     VARCHAR  COMMENT 'Numéro de volume',
    "1er lot"                       VARCHAR  COMMENT 'Numéro du 1er lot',
    "Surface Carrez du 1er lot"     VARCHAR  COMMENT 'Surface loi Carrez lot 1 (m²)',
    "2eme lot"                      VARCHAR  COMMENT 'Numéro du 2ème lot',
    "Surface Carrez du 2eme lot"    VARCHAR  COMMENT 'Surface loi Carrez lot 2 (m²)',
    "3eme lot"                      VARCHAR  COMMENT 'Numéro du 3ème lot',
    "Surface Carrez du 3eme lot"    VARCHAR  COMMENT 'Surface loi Carrez lot 3 (m²)',
    "4eme lot"                      VARCHAR  COMMENT 'Numéro du 4ème lot',
    "Surface Carrez du 4eme lot"    VARCHAR  COMMENT 'Surface loi Carrez lot 4 (m²)',
    "5eme lot"                      VARCHAR  COMMENT 'Numéro du 5ème lot',
    "Surface Carrez du 5eme lot"    VARCHAR  COMMENT 'Surface loi Carrez lot 5 (m²)',
    "Nombre de lots"                VARCHAR  COMMENT 'Nombre total de lots',
    "Code type local"               VARCHAR  COMMENT 'Code type de local',
    "Type local"                    VARCHAR  COMMENT 'Type de bien (maison, appartement...)',
    "Identifiant local"             VARCHAR  COMMENT 'Identifiant du local',
    "Surface reelle bati"           VARCHAR  COMMENT 'Surface bâtie réelle (m²)',
    "Nombre pieces principales"     VARCHAR  COMMENT 'Nombre de pièces principales',
    "Nature culture"                VARCHAR  COMMENT 'Nature de culture (terrain)',
    "Nature culture speciale"       VARCHAR  COMMENT 'Nature de culture spéciale',
    "Surface terrain"               VARCHAR  COMMENT 'Surface du terrain (m²)'
)
COMMENT = 'Table Bronze DVF — données brutes depuis S3. Chargement incrémental mensuel.'
DATA_RETENTION_TIME_IN_DAYS = 7;
