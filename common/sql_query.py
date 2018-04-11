multiflight_base_task_query = {
    'dept_city_sql': '''SELECT
              airport.iata_code,
              airport.inner_order,
              city.id,
              country.continent_id,
              city.name,
              city.tri_code
            FROM airport,  city, country 
            WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid  AND  city.name IN ('北京', '上海', '广州', '深圳', '香港');''',

    'dest_city_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM airport,  city, country 
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid  AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.id IN ({})'''
}

roundflight_base_task_query = {
    'rank1_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('北京', '上海', '广州', '深圳', '香港');''',

    'rank2_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM airport, city, country
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('成都', '武汉', '昆明', '郑州', '厦门', '南京', '重庆', '青岛', '长沙', '杭州', '天津', '大连', '西安', '长春');''',

    'rank3_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 0 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',

    'rank4_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';'''

}

flight_base_task_query = {
    'rank1_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('北京', '上海', '广州', '深圳', '香港');''',

    'rank2_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM airport, city, country
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('成都', '武汉', '昆明', '郑州', '厦门', '南京', '重庆', '青岛', '长沙', '杭州', '天津', '大连', '西安', '长春');''',

    'rank3_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 0 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',

    'rank4_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',

    'rank5_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = -1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';'''
}

temporary_flight_base_task_query = {
    'temporary_sql': '''select id from city where id>60183  and id<>90001;''',

    'rank3_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 0 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港' AND city.id IN ({});''',

    'rank4_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港' AND city.id IN ({});''',

    'rank5_sql': '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = -1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港' AND city.id IN ({});'''
}

hotel_base_task_query = {
    'base_sql': '''
   SELECT
         city.id AS city_id,
         city.trans_degree,
         city.grade,
         hotel_suggestions_city.source,
         hotel_suggestions_city.suggestions,
         hotel_suggestions_city.is_new_type
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0;''',

    'temp_sql': '''
    SELECT
         city.id AS city_id,
         city.trans_degree,
         city.grade,
         ota_location.source,
         ota_location.suggest,
         ota_location.suggest_type
       FROM ota_location
         LEFT JOIN city ON ota_location.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and city_id in ({}) ;
    '''
}

rail_base_task_query = {
    'base_sql': 'select city.id,city.country_id,ctrip_rail_table.code from city,ctrip_rail_table where ctrip_rail_table.cid=city.id and city.status_test="Open"',
    'rail_code_query': 'select cid, code from ctrip_rail_table where cid in ({})'
}

ferries_base_task_query = {
    'base_sql': 'select routeId from ferries_list;'
}

supervise_supplement_city_sql = {
    'flight_package_id_2': '''SELECT
  count(*) FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 0 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',

    'flight_package_id_3': '''SELECT
  count(*) FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',

    'flight_package_id_4': '''SELECT
  count(*)
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = -1 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';''',
    
    'flight_count_monitor': '''select count from task_monitor_supplement_city where package_id={}''',
    
    'update_flight_count': '''replace into task_monitor_supplement_city (package_id, count, datetime, type) value({}, {}, {}, 'Flight')'''
}
