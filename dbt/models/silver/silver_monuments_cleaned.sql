{{ config(materialized='table') }}

SELECT
    id,
    name,
    NULLIF(image, '')                          AS image,
    'UA' || substring(id, 1, 2)               AS adm1,
    substring(id, 1, 2)
        || CASE
             WHEN substring(id, 1, 2) IN ('80', '85') THEN '000'
             ELSE substring(id, 4, 3)
           END
        || '00000'                             AS adm2,
    NULLIF({{ clean_municipality('municipality') }}, '') AS municipality
FROM {{ ref('bronze_monuments_raw') }}
WHERE id IS NOT NULL
  AND id != ''
