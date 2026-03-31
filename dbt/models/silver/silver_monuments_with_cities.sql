{{ config(materialized='table') }}

WITH monuments AS (
    SELECT * FROM {{ ref('silver_monuments_cleaned') }}
),
katotth AS (
    SELECT * FROM {{ ref('silver_katotth_unique') }}
),
adm4_names AS (
    SELECT DISTINCT
        ADM4_PCODE AS code,
        ADM4_EN    AS adm4_name
    FROM {{ ref('bronze_humdata_raw') }}
    WHERE ADM4_PCODE IS NOT NULL
),
with_katotth AS (
    SELECT
        m.id,
        m.name,
        m.image,
        m.adm1,
        k.katotth
    FROM monuments m
    INNER JOIN katotth k
        ON substring(m.adm2, 1, 5) = k.koatuuPrefix
       AND m.municipality = k.name
),
with_adm4 AS (
    SELECT
        wk.id,
        wk.name,
        wk.image,
        wk.adm1,
        a.adm4_name                      AS municipality,
        substring(a.code, 1, 6)          AS adm2,
        substring(a.code, 1, 9)          AS adm3,
        a.code                           AS adm4
    FROM with_katotth wk
    INNER JOIN adm4_names a
        ON substring(wk.katotth, 1, 12) = a.code
)
SELECT id, name, municipality, image, adm1, adm2, adm3, adm4
FROM with_adm4
