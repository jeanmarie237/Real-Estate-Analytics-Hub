{{ config(materialized='table') }}

WITH annuel AS (
    SELECT
        YEAR(date_mutation)                                         AS annee,
        type_local,
        COUNT(*)                                                    AS nb_transactions,
        AVG(valeur_fonciere)                                        AS prix_moyen,
        MEDIAN(valeur_fonciere)                                     AS prix_median,
        AVG(valeur_fonciere / NULLIF(surface_reelle_bati, 0))       AS prix_m2_moyen,
        SUM(valeur_fonciere)                                        AS volume_financier
    FROM {{ ref('silver_mutation_f') }}
    WHERE valeur_fonciere IS NOT NULL
      AND date_mutation IS NOT NULL
    GROUP BY annee, type_local
),

avec_variation AS (
    SELECT
        annee,
        type_local,
        nb_transactions,
        prix_moyen,
        prix_median,
        prix_m2_moyen,
        volume_financier,
        LAG(prix_moyen)       OVER (PARTITION BY type_local ORDER BY annee) AS prix_moyen_an_precedent,
        LAG(nb_transactions)  OVER (PARTITION BY type_local ORDER BY annee) AS nb_transactions_an_precedent
    FROM annuel
)

SELECT
    annee,
    type_local,
    nb_transactions,
    prix_moyen,
    prix_median,
    prix_m2_moyen,
    volume_financier,
    ROUND(
        (prix_moyen - prix_moyen_an_precedent) / NULLIF(prix_moyen_an_precedent, 0) * 100,
        2
    )                                                               AS variation_prix_pct,
    ROUND(
        (nb_transactions - nb_transactions_an_precedent) / NULLIF(nb_transactions_an_precedent, 0) * 100,
        2
    )                                                               AS variation_volume_pct
FROM avec_variation
ORDER BY annee, type_local
