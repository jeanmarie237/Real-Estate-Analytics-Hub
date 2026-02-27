{{ config(materialized='table', schema='STAR') }}

/*
  Table de faits centrale du modèle en étoile.
  Granularité : 1 ligne = 1 transaction immobilière (mutation_id unique).

  Clés étrangères vers :
    - dim_date          (date_key)
    - dim_geography     (geo_key)
    - dim_type_bien     (type_bien_key)
    - dim_nature_mutation (nature_key)
*/

SELECT
    -- Clé primaire
    f.mutation_id,

    -- Clés étrangères (FK vers les dimensions)
    TO_NUMBER(TO_CHAR(f.date_mutation, 'YYYYMMDD'))
        AS date_key,

    {{ dbt_utils.generate_surrogate_key(['f.commune', 'f.code_postal', 'f.code_departement']) }}
        AS geo_key,

    {{ dbt_utils.generate_surrogate_key(['f.type_local']) }}
        AS type_bien_key,

    {{ dbt_utils.generate_surrogate_key(['f.nature_mutation']) }}
        AS nature_key,

    -- Mesures (faits quantitatifs)
    f.valeur_fonciere,
    f.surface_reelle_bati,
    f.surface_terrain,
    f.nombre_pieces_principales,
    f.prix_metre_carre,

    -- Mesures dérivées utiles pour Power BI
    CASE WHEN f.valeur_fonciere IS NOT NULL THEN 1 ELSE 0 END  AS est_vente_avec_prix,
    CASE WHEN f.surface_reelle_bati > 0    THEN 1 ELSE 0 END  AS est_bien_bati

FROM {{ ref('silver_mutation_f') }} f
WHERE f.date_mutation IS NOT NULL
