{{ config(materialized='table') }}

SELECT
    type_local,
    ROUND(surface_reelle_bati, 1) AS tranche_surface,
    COUNT(*) AS nb_transactions,
    AVG(valeur_fonciere) AS prix_moyen
FROM {{ ref('silver_mutation_f') }}
WHERE surface_reelle_bati > 0
GROUP BY type_local, tranche_surface
ORDER BY tranche_surface
