{{ config(materialized='table') }}

SELECT *
FROM read_csv(
  'data/raw/humdata.csv',
  header = true,
  all_varchar = true,
  nullstr = ''
)
