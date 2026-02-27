{{ config(materialized='table') }}

-- KPIs globaux pour les cartes de synthèse du dashboard
SELECT
    COUNT(*)                                                        AS total_transactions,
    COUNT(DISTINCT commune)                                         AS nb_communes,
    COUNT(DISTINCT code_departement)                                AS nb_departements,
    MIN(date_mutation)                                              AS date_premiere_transaction,
    MAX(date_mutation)                                              AS date_derniere_transaction,
    SUM(valeur_fonciere)                                            AS volume_financier_total,
    AVG(valeur_fonciere)                                            AS prix_moyen_france,
    MEDIAN(valeur_fonciere)                                         AS prix_median_france,
    AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0))           AS prix_m2_moyen_france
FROM {{ ref('silver_mutation_f') }}
WHERE valeur_fonciere IS NOT NULL
