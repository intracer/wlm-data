Monuments database
Source: http://lists.wlm.photo
Available under the Creative Commons Attribution-ShareAlike License (https://creativecommons.org/licenses/by-sa/4.0/)

The database contains machine-readable data on different objects of cultural heritage value from Wiki Loves Monuments Ukraine (country='ua').

Database structure: https://commons.wikimedia.org/wiki/Commons:Monuments_database/Database_structure

Mapping from source pages is done using the following two level mapping (fields and sql_data entries): https://github.com/wikimedia/labs-tools-heritage/blob/master/erfgoedbot/monuments_config/ua_uk.json

For country='ua', monument id has format 'adm1'-'adm2'-xxxx, where 
* adm1 - two digits, first level code according to KOATTUU (https://en.wikipedia.org/wiki/Classification_of_objects_of_the_administrative-territorial_system_of_Ukraine)
* adm2 - three digits, second level code according to KOATTUU

For example:
* Odesa region has KOATTUU code of 5100000000, 
  so all monuments in Odesa region has id of type 51-adm2-dddd, 
* Odesa city is located in Odesa region, 
  has KOATTUU code of 5110100000, 
  where adm1 = 51, adm2 = 101, 
  so all monuments in Odesa city has id of type 51-101-dddd

Database was initiated in 2012 while KOATTUU was in place, and KOATTUU was replaced in 2020 with a new classifier KATOTTH (https://en.wikipedia.org/wiki/Codifier_of_administrative-territorial_units_and_territories_of_territorial_communities)
so mapping between KOATTUU and KATOTTH needs to be done.