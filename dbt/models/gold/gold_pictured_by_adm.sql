{{ config(materialized='table') }}

WITH monuments AS (
    SELECT * FROM {{ ref('silver_monuments_with_cities') }}
),
adm_names AS (
    SELECT DISTINCT 'ADM1' AS level, ADM1_PCODE AS code, ADM1_EN AS adm_name
    FROM {{ ref('bronze_humdata_raw') }} WHERE ADM1_PCODE IS NOT NULL
    UNION ALL
    SELECT DISTINCT 'ADM2', ADM2_PCODE, ADM2_EN
    FROM {{ ref('bronze_humdata_raw') }} WHERE ADM2_PCODE IS NOT NULL
    UNION ALL
    SELECT DISTINCT 'ADM3', ADM3_PCODE, ADM3_EN
    FROM {{ ref('bronze_humdata_raw') }} WHERE ADM3_PCODE IS NOT NULL
    UNION ALL
    SELECT DISTINCT 'ADM4', ADM4_PCODE, ADM4_EN
    FROM {{ ref('bronze_humdata_raw') }} WHERE ADM4_PCODE IS NOT NULL
),
monument_by_level AS (
    SELECT id, image, 'ADM1' AS level, adm1 AS code FROM monuments
    UNION ALL
    SELECT id, image, 'ADM2', adm2 FROM monuments
    UNION ALL
    SELECT id, image, 'ADM3', adm3 FROM monuments
    UNION ALL
    SELECT id, image, 'ADM4', adm4 FROM monuments
),
agg AS (
    SELECT
        n.level,
        n.code,
        n.adm_name                            AS name,
        COUNT(m.id)                           AS total,
        COUNT(CASE WHEN m.image IS NOT NULL AND m.image != '' THEN 1 END) AS pictured
    FROM monument_by_level m
    JOIN adm_names n ON m.level = n.level AND m.code = n.code
    GROUP BY n.level, n.code, n.adm_name
)
SELECT
    level,
    code,
    name,
    total,
    pictured,
    ROUND(100.0 * pictured / NULLIF(total, 0), 2) AS percentage
FROM agg
ORDER BY level, percentage DESC
