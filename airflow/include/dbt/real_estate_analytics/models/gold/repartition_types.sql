{{ config(materialized='table') }}

SELECT
    type_local,
    COUNT(*) AS nb_transactions
FROM {{ ref('silver_mutation_f') }}
GROUP BY type_local
ORDER BY nb_transactions DESC
