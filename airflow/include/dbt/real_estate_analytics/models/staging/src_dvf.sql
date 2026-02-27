
{{ config(materialized='view') }}

SELECT
    "No disposition"                 AS no_disposition,
    "Identifiant de document"        AS identifiant_document,
    "Reference document"             AS reference_document,
    "Date mutation"                  AS date_mutation,
    "Nature mutation"                AS nature_mutation,
    "Valeur fonciere"                AS valeur_fonciere,
    "Code postal"                    AS code_postal,
    "Commune"                        AS commune,
    "Code departement"               AS code_departement,
    "Type local"                     AS type_local,
    "Surface reelle bati"            AS surface_reelle_bati,
    "Nombre pieces principales"      AS nombre_pieces_principales,
    "Surface terrain"                AS surface_terrain
FROM {{ source('dvf', 'mutations_foncieres') }}


