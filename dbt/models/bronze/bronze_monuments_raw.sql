{{ config(materialized='table') }}

SELECT *
FROM read_csv(
  'data/raw/monuments.csv',
  header = true,
  all_varchar = true,
  nullstr = ''
)
