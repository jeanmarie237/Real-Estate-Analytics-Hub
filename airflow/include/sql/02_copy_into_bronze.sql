-- ============================================================
-- Script : 02_copy_into_bronze.sql
-- Description : Chargement incrémental S3 → Snowflake DEV_BRONZE
--
-- Stratégie : FILE_FORMAT inline (pas le format nommé) avec PARSE_HEADER=TRUE
--   → compatible MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
--   → évite tout conflit avec SKIP_HEADER du format nommé
--
-- Idempotence : FORCE = FALSE (Snowflake skip les fichiers déjà chargés
--   via son registre interne COPY_HISTORY)
-- Erreurs     : ON_ERROR = CONTINUE (rows invalides ignorées)
-- ============================================================

USE DATABASE DVF_DB;
USE WAREHOUSE DVF_WH;
USE ROLE DVF_BI_ROLE;

COPY INTO DVF_DB.DEV_BRONZE.mutations_foncieres
FROM @DVF_DB.DEV_BRONZE.dvf_s3_stage
FILE_FORMAT = (
    TYPE                           = 'CSV'
    FIELD_DELIMITER                = '|'
    PARSE_HEADER                   = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    ENCODING                       = 'UTF8'
    TRIM_SPACE                     = TRUE
    EMPTY_FIELD_AS_NULL            = TRUE
    NULL_IF                        = ('', 'NULL', 'null')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR             = CONTINUE
PURGE                = FALSE
FORCE                = FALSE;
