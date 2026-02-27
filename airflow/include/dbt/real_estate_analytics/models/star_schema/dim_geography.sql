{{ config(materialized='table', schema='STAR') }}

WITH communes_distinctes AS (
    SELECT DISTINCT
        COALESCE(commune, 'Inconnu')          AS commune,
        COALESCE(code_postal, 'XXXXX')        AS code_postal,
        COALESCE(code_departement, 'XX')      AS code_departement
    FROM {{ ref('silver_mutation_f') }}
),

enriched AS (
    SELECT
        c.commune,
        c.code_postal,
        c.code_departement,
        COALESCE(d.nom_departement, 'Département ' || c.code_departement) AS nom_departement,
        COALESCE(d.region, 'Inconnue')                                     AS region,
        -- Regroupements analytiques
        CASE
            WHEN c.code_departement IN ('75', '77', '78', '91', '92', '93', '94', '95')
                THEN 'Île-de-France'
            WHEN c.code_departement IN ('06', '13', '83', '84')
                THEN 'PACA'
            WHEN c.code_departement IN ('33', '31', '34', '13', '69')
                THEN 'Grandes métropoles'
            ELSE COALESCE(d.region, 'Autre')
        END                                                                AS zone_analytique
    FROM communes_distinctes c
    LEFT JOIN {{ ref('ref_departements') }} d
        ON c.code_departement = d.code_departement
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['commune', 'code_postal', 'code_departement']) }} AS geo_key,
    commune,
    code_postal,
    code_departement,
    nom_departement,
    region,
    zone_analytique
FROM enriched
