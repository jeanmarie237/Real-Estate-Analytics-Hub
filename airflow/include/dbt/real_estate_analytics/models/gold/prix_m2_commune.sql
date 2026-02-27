{{ config(materialized='table') }}

SELECT
    commune,
    code_postal,
    type_local,
    COUNT(*) AS nb_transactions,
    AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0)) AS prix_m2_moyen
FROM {{ ref('silver_mutation_f') }}
WHERE surface_reelle_bati > 0
  AND valeur_fonciere IS NOT NULL
GROUP BY commune, code_postal, type_local
