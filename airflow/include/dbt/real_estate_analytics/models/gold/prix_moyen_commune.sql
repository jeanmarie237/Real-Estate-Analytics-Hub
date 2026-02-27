{{ config(materialized='table') }}

SELECT
    commune,
    code_postal,
    type_local,
    COUNT(*)                       AS nb_transactions,
    AVG(valeur_fonciere)           AS prix_moyen,
    MEDIAN(valeur_fonciere)        AS prix_median
FROM {{ ref('silver_mutation_f') }}
WHERE valeur_fonciere IS NOT NULL
GROUP BY commune, code_postal, type_local
