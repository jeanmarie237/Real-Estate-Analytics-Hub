{{ config(materialized='table', schema='STAR') }}

WITH types_distincts AS (
    SELECT DISTINCT type_local
    FROM {{ ref('silver_mutation_f') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['type_local']) }}   AS type_bien_key,
    COALESCE(type_local, 'non renseigné')                    AS type_local,

    -- Catégorie métier
    CASE
        WHEN type_local IN ('maison', 'appartement')
            THEN 'Résidentiel'
        WHEN type_local = 'local industriel. commercial ou assimilé'
            THEN 'Commercial'
        WHEN type_local = 'dépendance'
            THEN 'Dépendance'
        WHEN type_local IS NULL
            THEN 'Non renseigné (terrain)'
        ELSE 'Autre'
    END                                                      AS categorie,

    -- Libellé lisible pour Power BI
    CASE
        WHEN type_local = 'maison'                                         THEN 'Maison'
        WHEN type_local = 'appartement'                                    THEN 'Appartement'
        WHEN type_local = 'local industriel. commercial ou assimilé'       THEN 'Local commercial'
        WHEN type_local = 'dépendance'                                     THEN 'Dépendance'
        WHEN type_local IS NULL                                            THEN 'Terrain / Non renseigné'
        ELSE INITCAP(type_local)
    END                                                      AS libelle,

    -- Ordre d'affichage pour les graphiques
    CASE
        WHEN type_local = 'maison'                                         THEN 1
        WHEN type_local = 'appartement'                                    THEN 2
        WHEN type_local = 'local industriel. commercial ou assimilé'       THEN 3
        WHEN type_local = 'dépendance'                                     THEN 4
        ELSE 5
    END                                                      AS ordre_affichage

FROM types_distincts
