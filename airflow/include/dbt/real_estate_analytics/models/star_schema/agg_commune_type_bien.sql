{{ config(materialized='table', schema='STAR') }}

/*
  Agrégation commune × type de bien × année.
  Grain : 1 ligne = 1 commune × 1 type_bien_key × 1 année
  Cardinalité : ~100 000–150 000 lignes (filtre min. 5 transactions)

  Filtre actif : uniquement communes avec >= 5 transactions/an/type
  → Supprime le bruit des micro-marchés et réduit la cardinalité de ~80%.

  Alimente le benchmark communes et la table de détail page 4.
*/

WITH base AS (
    SELECT
        g.geo_key,
        g.commune,
        g.code_postal,
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
        MIN(f.valeur_fonciere)                                      AS prix_min,
        MAX(f.valeur_fonciere)                                      AS prix_max,
        AVG(f.prix_metre_carre)                                     AS prix_m2_moyen,
        AVG(f.surface_reelle_bati)                                  AS surface_moyenne
    FROM {{ ref('fact_mutations') }} f
    JOIN {{ ref('dim_geography') }} g  ON f.geo_key  = g.geo_key
    JOIN {{ ref('dim_date') }}      d  ON f.date_key = d.date_key
    GROUP BY
        g.geo_key, g.commune, g.code_postal,
        g.code_departement, g.nom_departement, g.region,
        f.type_bien_key, d.annee
    HAVING COUNT(*) >= 5
)

SELECT * FROM base
