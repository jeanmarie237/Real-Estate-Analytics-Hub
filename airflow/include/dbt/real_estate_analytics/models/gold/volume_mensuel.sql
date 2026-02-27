{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('month', date_mutation) AS mois,
    COUNT(*)                           AS nb_transactions,
    SUM(valeur_fonciere)               AS volume_financier
FROM {{ ref('silver_mutation_f') }}
GROUP BY mois
ORDER BY mois
