{{ config(materialized='table', schema='STAR') }}

/*
  Agrégation département × type de bien × année.
  Grain : 1 ligne = 1 département × 1 type_bien_key × 1 année
  Cardinalité : ~3 000 lignes (101 depts × 5 types × 6 ans)

  Alimente la carte choroplèthe et le benchmark départemental.
  Inclut variation YoY calculée en SQL (pas besoin de DAX DATEADD).
*/

WITH base AS (
    SELECT
        g.code_departement,
        g.nom_departement,
        g.region,
        f.type_bien_key,
        d.annee,
        COUNT(*)                                                    AS nb_transactions,
        COUNT(CASE WHEN f.est_vente_avec_prix = 1 THEN 1 END)      AS nb_ventes,
        SUM(f.valeur_fonciere)                                      AS volume_financier,
        AVG(f.valeur_fonciere)                                      AS prix_moyen,
        MEDIAN(f.valeur_fonciere)                                   AS prix_median,
        AVG(f.prix_metre_carre)                                     AS prix_m2_moyen
    FROM {{ ref('fact_mutations') }} f
    JOIN {{ ref('dim_geography') }} g  ON f.geo_key      = g.geo_key
    JOIN {{ ref('dim_date') }}      d  ON f.date_key     = d.date_key
    GROUP BY g.code_departement, g.nom_departement, g.region, f.type_bien_key, d.annee
),

avec_yoy AS (
    SELECT
        *,
        LAG(prix_moyen)      OVER (PARTITION BY code_departement, type_bien_key ORDER BY annee)
            AS prix_moyen_n1,
        LAG(nb_transactions) OVER (PARTITION BY code_departement, type_bien_key ORDER BY annee)
            AS nb_transactions_n1
    FROM base
)

SELECT
    code_departement,
    nom_departement,
    region,
    type_bien_key,
    annee,
    nb_transactions,
    nb_ventes,
    volume_financier,
    prix_moyen,
    prix_median,
    prix_m2_moyen,
    ROUND(
        (prix_moyen - prix_moyen_n1) / NULLIF(prix_moyen_n1, 0) * 100, 2
    )                                                               AS variation_prix_yoy_pct,
    ROUND(
        (nb_transactions - nb_transactions_n1) / NULLIF(nb_transactions_n1, 0) * 100, 2
    )                                                               AS variation_volume_yoy_pct
FROM avec_yoy
