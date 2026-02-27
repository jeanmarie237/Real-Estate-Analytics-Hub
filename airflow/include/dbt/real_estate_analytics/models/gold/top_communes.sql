{{ config(materialized='table') }}

SELECT
    commune,
    code_postal,
    AVG(valeur_fonciere) AS prix_moyen
FROM {{ ref('silver_mutation_f') }}
GROUP BY commune, code_postal
HAVING COUNT(*) > 50
ORDER BY prix_moyen DESC
LIMIT 50
