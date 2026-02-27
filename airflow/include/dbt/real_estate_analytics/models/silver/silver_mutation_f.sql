{{
  config(
    materialized='incremental',
    unique_key='mutation_id'
  )
}}

WITH source AS (
    SELECT DISTINCT *
    FROM {{ ref('src_dvf') }}
),

transformed AS (
    SELECT
        no_disposition,
        identifiant_document,
        TRY_TO_DATE(date_mutation)                                    AS date_mutation,
        LOWER(nature_mutation)                                        AS nature_mutation,
        TRY_TO_NUMBER(REPLACE(valeur_fonciere, ',', '.'))             AS valeur_fonciere,
        code_postal,
        LOWER(commune)                                                AS commune,
        code_departement,
        LOWER(type_local)                                             AS type_local,
        TRY_TO_NUMBER(REPLACE(surface_reelle_bati, ',', '.'))         AS surface_reelle_bati,
        nombre_pieces_principales,
        CAST(TRY_TO_NUMBER(REPLACE(surface_terrain, ',', '.')) AS INT) AS surface_terrain
    FROM source
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                no_disposition, 
                identifiant_document, 
                COALESCE(type_local, ''), 
                COALESCE(surface_reelle_bati::STRING, '')
            ORDER BY no_disposition
        ) AS lot_rank
    FROM transformed
),

filtered AS (
    SELECT *
    FROM ranked
    WHERE
        -- Suppression des prix nuls ou négatifs
        valeur_fonciere IS NOT NULL
        AND valeur_fonciere > 0

        -- Règles spécifiques par type de bien
        AND CASE
            WHEN type_local IN ('maison', 'appartement')
                THEN valeur_fonciere >= 1000
                    AND (surface_reelle_bati >= 9 OR surface_reelle_bati IS NULL)
            WHEN type_local = 'dépendance'
                THEN valeur_fonciere >= 500
            WHEN type_local = 'local industriel. commercial ou assimilé'
                THEN valeur_fonciere >= 1000
                    AND (surface_reelle_bati >= 5 OR surface_reelle_bati IS NULL)
            ELSE valeur_fonciere >= 1000  -- non renseigné et autres cas
        END
),

silver_with_id AS (
    SELECT
        no_disposition,
        identifiant_document,
        date_mutation,
        nature_mutation,
        valeur_fonciere,
        code_postal,
        commune,
        code_departement,
        type_local,
        surface_reelle_bati,
        nombre_pieces_principales,
        surface_terrain,

        -- Génération de l'identifiant unique
        MD5(
            CONCAT(
                COALESCE(no_disposition::STRING, ''),
                COALESCE(identifiant_document::STRING, ''),
                COALESCE(date_mutation::STRING, ''),
                COALESCE(valeur_fonciere::STRING, ''),
                COALESCE(code_postal::STRING, ''),
                COALESCE(commune, ''),
                COALESCE(type_local, ''),
                COALESCE(surface_reelle_bati::STRING, ''),
                COALESCE(surface_terrain::STRING, ''),
                COALESCE(nombre_pieces_principales::STRING, ''),
                lot_rank::STRING
            )
        ) AS mutation_id,

        -- Calcul du prix au m² selon le type de bien
        CASE
            WHEN type_local IN ('maison', 'appartement', 'local industriel. commercial ou assimilé')
                AND surface_reelle_bati > 0
                THEN valeur_fonciere / NULLIF(surface_reelle_bati, 0)
            WHEN type_local = 'dépendance'
                THEN valeur_fonciere / NULLIF(COALESCE(surface_reelle_bati, surface_terrain), 0)
            ELSE NULL
        END AS prix_metre_carre

    FROM filtered
)

SELECT * FROM silver_with_id

{% if is_incremental() %}
WHERE date_mutation > (SELECT MAX(date_mutation) FROM {{ this }})
{% endif %}