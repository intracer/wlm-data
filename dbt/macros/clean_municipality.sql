{% macro clean_municipality(col) %}
TRIM(
  SPLIT_PART(
    SPLIT_PART(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
      replace(replace(replace(
        {{ col }},
        'р-н',            'район'),
        'сільська рада',  ''),
        'селищна рада',   ''),
        '[[',             ''),
        ']]',             ''),
        '&nbsp;',         ' '),
        chr(160),         ' '),
        'м.',             ''),
        'місто',          ''),
        'с.',             ''),
        'С.',             ''),
        '.',              ''),
        'село',           ''),
        'сел.',           ''),
        'смт',            ''),
        'Смт',            ''),
        'с-ще',           ''),
        'с-щ',            ''),
        chr(39)||chr(39)||chr(39), ''),
        chr(39)||chr(39),         ''),
        ',',              ''),
        chr(8217),        chr(39)),
        chr(8220),        chr(39)),
    '(', 1),
  '|', 1)
)
{% endmacro %}
