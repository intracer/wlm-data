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
    SELECT id, 'ADM1' AS level, adm1 AS code FROM monuments
    UNION ALL
    SELECT id, 'ADM2', adm2 FROM monuments
    UNION ALL
    SELECT id, 'ADM3', adm3 FROM monuments
    UNION ALL
    SELECT id, 'ADM4', adm4 FROM monuments
)
SELECT
    n.level,
    n.code,
    n.adm_name AS name,
    COUNT(m.id) AS monument_count
FROM monument_by_level m
JOIN adm_names n ON m.level = n.level AND m.code = n.code
GROUP BY n.level, n.code, n.adm_name
ORDER BY n.level, monument_count DESC
