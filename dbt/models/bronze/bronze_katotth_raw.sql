{{ config(materialized='table') }}

SELECT *
FROM read_csv(
  'data/katotth/katotth_koatuu.csv',
  header = true,
  all_varchar = true,
  nullstr = ''
)
