{{ config(materialized='table', schema='STAR') }}

/*
  Agrégation annuelle × type de bien — France entière.
  Grain : 1 ligne = 1 année × 1 type_bien_key
  Cardinalité : ~30 lignes (6 ans × 5 types)

  Table ultra-légère pour les KPI cards et comparaisons YoY
  sans aucun calcul DAX complexe côté Power BI.
*/

WITH base AS (
    SELECT
        f.type_bien_key,
        d.annee,
        COUNT(*)                                                    AS nb_transactions,
        COUNT(CASE WHEN f.est_vente_avec_prix = 1 THEN 1 END)      AS nb_ventes,
        SUM(f.valeur_fonciere)                                      AS volume_financier,
        AVG(f.valeur_fonciere)                                      AS prix_moyen,
        MEDIAN(f.valeur_fonciere)                                   AS prix_median,
        MIN(f.valeur_fonciere)                                      AS prix_min,
        MAX(f.valeur_fonciere)                                      AS prix_max,
        AVG(f.prix_metre_carre)                                     AS prix_m2_moyen,
        AVG(f.surface_reelle_bati)                                  AS surface_moyenne,
        COUNT(DISTINCT f.geo_key)                                   AS nb_communes_actives
    FROM {{ ref('fact_mutations') }} f
    JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
    GROUP BY f.type_bien_key, d.annee
),

avec_yoy AS (
    SELECT
        *,
        LAG(prix_moyen)      OVER (PARTITION BY type_bien_key ORDER BY annee) AS prix_moyen_n1,
        LAG(nb_transactions) OVER (PARTITION BY type_bien_key ORDER BY annee) AS nb_transactions_n1,
        LAG(volume_financier) OVER (PARTITION BY type_bien_key ORDER BY annee) AS volume_n1
    FROM base
)

SELECT
    type_bien_key,
    annee,
    nb_transactions,
    nb_ventes,
    volume_financier,
    prix_moyen,
    prix_median,
    prix_min,
    prix_max,
    prix_m2_moyen,
    surface_moyenne,
    nb_communes_actives,
    ROUND(
        (prix_moyen - prix_moyen_n1) / NULLIF(prix_moyen_n1, 0) * 100, 2
    )                                                               AS variation_prix_yoy_pct,
    ROUND(
        (nb_transactions - nb_transactions_n1) / NULLIF(nb_transactions_n1, 0) * 100, 2
    )                                                               AS variation_volume_yoy_pct,
    ROUND(
        (volume_financier - volume_n1) / NULLIF(volume_n1, 0) * 100, 2
    )                                                               AS variation_volume_financier_yoy_pct
FROM avec_yoy
ORDER BY annee, type_bien_key
