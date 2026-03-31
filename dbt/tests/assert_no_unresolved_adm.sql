-- Fails if any monument has unresolved adm2, adm3, or adm4
-- A non-empty result = test failure in dbt
SELECT id, adm1, adm2, adm3, adm4
FROM {{ ref('silver_monuments_with_cities') }}
WHERE adm2 IS NULL
   OR adm3 IS NULL
   OR adm4 IS NULL
