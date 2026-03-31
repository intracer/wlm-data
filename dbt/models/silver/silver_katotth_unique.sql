{{ config(materialized='table') }}

WITH grouped AS (
    SELECT
        substring(koatuu, 1, 5) AS koatuuPrefix,
        name,
        COUNT(*)                AS cnt,
        MAX(katotth)            AS katotth,
        MAX(koatuu)             AS koatuu,
        MAX(category)           AS category
    FROM {{ ref('bronze_katotth_raw') }}
    GROUP BY substring(koatuu, 1, 5), name
)
SELECT koatuuPrefix, name, katotth, koatuu, category
FROM grouped
WHERE cnt = 1
