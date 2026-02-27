{{ config(materialized='table', schema='STAR') }}

WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01')::DATE AS date_complete
    FROM TABLE(GENERATOR(ROWCOUNT => 2557))  -- 2020-01-01 → 2026-12-31
),

enriched AS (
    SELECT
        TO_NUMBER(TO_CHAR(date_complete, 'YYYYMMDD'))   AS date_key,
        date_complete,
        YEAR(date_complete)                              AS annee,
        QUARTER(date_complete)                           AS trimestre_num,
        CONCAT('T', QUARTER(date_complete))              AS trimestre_libelle,
        MONTH(date_complete)                             AS mois_num,
        TO_CHAR(date_complete, 'MMMM')                  AS mois_nom,
        TO_CHAR(date_complete, 'MON YYYY')              AS mois_annee,
        WEEKOFYEAR(date_complete)                        AS semaine_annee,
        DAYOFWEEK(date_complete)                         AS jour_semaine_num,
        TO_CHAR(date_complete, 'DY')                    AS jour_semaine_nom,
        IFF(DAYOFWEEK(date_complete) IN (6, 7), TRUE, FALSE) AS is_weekend,
        CONCAT('T', QUARTER(date_complete), ' ', YEAR(date_complete)) AS libelle_trimestre_annee,
        -- Semestre
        IFF(MONTH(date_complete) <= 6, 'S1', 'S2')     AS semestre
    FROM date_spine
)

SELECT * FROM enriched
