{{ config(materialized='table') }}

SELECT
    m.id,
    m.name,
    m.municipality,
    m.image,
    m.adm1,
    m.adm2,
    m.adm3,
    m.adm4,
    a1.ADM1_EN AS adm1_name,
    a2.ADM2_EN AS adm2_name,
    a3.ADM3_EN AS adm3_name
FROM {{ ref('silver_monuments_with_cities') }} m
LEFT JOIN (SELECT DISTINCT ADM1_PCODE, ADM1_EN FROM {{ ref('bronze_humdata_raw') }}) a1
    ON m.adm1 = a1.ADM1_PCODE
LEFT JOIN (SELECT DISTINCT ADM2_PCODE, ADM2_EN FROM {{ ref('bronze_humdata_raw') }}) a2
    ON m.adm2 = a2.ADM2_PCODE
LEFT JOIN (SELECT DISTINCT ADM3_PCODE, ADM3_EN FROM {{ ref('bronze_humdata_raw') }}) a3
    ON m.adm3 = a3.ADM3_PCODE
