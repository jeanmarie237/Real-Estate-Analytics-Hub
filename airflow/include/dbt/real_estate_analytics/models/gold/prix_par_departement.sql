{{ config(materialized='table') }}

SELECT
    code_departement,
    type_local,
    COUNT(*)                                                        AS nb_transactions,
    AVG(valeur_fonciere)                                            AS prix_moyen,
    MEDIAN(valeur_fonciere)                                         AS prix_median,
    AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0))           AS prix_m2_moyen,
    SUM(valeur_fonciere)                                            AS volume_financier
FROM {{ ref('silver_mutation_f') }}
WHERE valeur_fonciere IS NOT NULL
  AND code_departement IS NOT NULL
GROUP BY code_departement, type_local
