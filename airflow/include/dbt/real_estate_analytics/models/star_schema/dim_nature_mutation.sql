{{ config(materialized='table', schema='STAR') }}

WITH natures_distinctes AS (
    SELECT DISTINCT nature_mutation
    FROM {{ ref('silver_mutation_f') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['nature_mutation']) }}  AS nature_key,
    COALESCE(nature_mutation, 'inconnue')                        AS nature_mutation,

    -- Famille métier (pour filtres et regroupements)
    CASE
        WHEN nature_mutation LIKE '%vente%'                  THEN 'Vente'
        WHEN nature_mutation LIKE '%adjudication%'           THEN 'Vente judiciaire'
        WHEN nature_mutation LIKE '%expropriation%'          THEN 'Expropriation'
        WHEN nature_mutation LIKE '%échange%'                THEN 'Échange'
        WHEN nature_mutation LIKE '%donation%'               THEN 'Donation'
        WHEN nature_mutation IS NULL                         THEN 'Inconnue'
        ELSE 'Autre'
    END                                                          AS famille,

    -- Flag vente marchande (pour filtrer les prix de marché)
    IFF(
        nature_mutation LIKE '%vente%'
        OR nature_mutation LIKE '%adjudication%',
        TRUE,
        FALSE
    )                                                            AS is_vente_marchande

FROM natures_distinctes
