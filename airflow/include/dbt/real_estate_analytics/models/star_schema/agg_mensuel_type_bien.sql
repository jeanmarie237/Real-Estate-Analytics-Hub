{{ config(materialized='table', schema='STAR') }}

/*
  Agrégation mensuelle × type de bien.
  Grain : 1 ligne = 1 mois × 1 type_bien_key
  Cardinalité : ~3 000 lignes (6 ans × 12 mois × 5 types)

  Remplace l'import direct de fact_mutations pour tous les visuels
  temporels du dashboard Power BI (page Évolution + page Accueil).
*/

SELECT
    -- Clés dimensions
    f.type_bien_key,
    TO_NUMBER(TO_CHAR(DATE_TRUNC('month', d.date_complete), 'YYYYMMDD'))  AS date_mois_key,
    DATE_TRUNC('month', d.date_complete)                                   AS mois,
    d.annee,
    d.trimestre_num,
    d.trimestre_libelle,
    d.mois_num,
    d.mois_nom,
    d.semestre,

    -- Mesures agrégées
    COUNT(*)                                                               AS nb_transactions,
    COUNT(CASE WHEN f.est_vente_avec_prix = 1 THEN 1 END)                 AS nb_ventes,
    SUM(f.valeur_fonciere)                                                 AS volume_financier,
    AVG(f.valeur_fonciere)                                                 AS prix_moyen,
    MEDIAN(f.valeur_fonciere)                                              AS prix_median,
    AVG(f.prix_metre_carre)                                                AS prix_m2_moyen,
    AVG(f.surface_reelle_bati)                                             AS surface_moyenne,
    SUM(f.est_bien_bati)                                                   AS nb_biens_batis

FROM {{ ref('fact_mutations') }} f
JOIN {{ ref('dim_date') }} d
    ON f.date_key = d.date_key
GROUP BY
    f.type_bien_key,
    DATE_TRUNC('month', d.date_complete),
    d.annee,
    d.trimestre_num,
    d.trimestre_libelle,
    d.mois_num,
    d.mois_nom,
    d.semestre
