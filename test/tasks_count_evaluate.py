from mysql_execute import fetch_count
from conn_pool import source_info_pool

rank_1 = '''  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and (city.trans_degree=0 or city.trans_degree=1 or city.grade=1 or city.grade=2)  group by city.id) as t ;
    '''

rank_1_single = '''  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and (city.trans_degree=0 or city.trans_degree=1 or city.grade=1 or city.grade=2)  group by city.id having num =1) as t ;'''

rank_2 = '''  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=3 or city.grade=4)    group by city.id) as t ;
'''

rank_2_single = '''  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=3 or city.grade=4)    group by city.id having num=1) as t ;
       '''

rank_3 = '''  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=5 or city.grade=6)    group by city.id ) as t ;
       '''

rank_3_single = '''  
  select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=5 or city.grade=6)    group by city.id having num=1) as t ;
'''

rank_4 = '''
select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=7 or city.grade=8 or city.grade=9)  group by city.id) as t ;
'''

rank_4_single = '''select count(num) from (SELECT
         city.id,count(*) as num
       FROM hotel_suggestions_city
         LEFT JOIN city ON hotel_suggestions_city.city_id= city.id
       WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') and select_index>0 and  ( city.grade=7 or city.grade=8 or city.grade=9)  group by city.id having num=1) as t ;
       '''

rank_1_count = fetch_count(source_info_pool, rank_1)
rank_1_single_count = fetch_count(source_info_pool, rank_1_single)
rank_2_count = fetch_count(source_info_pool, rank_2)
rank_2_single_count = fetch_count(source_info_pool, rank_2_single)
rank_3_count = fetch_count(source_info_pool, rank_3)
rank_3_single_count = fetch_count(source_info_pool, rank_3_single)
rank_4_count = fetch_count(source_info_pool, rank_4)
rank_4_single_count = fetch_count(source_info_pool, rank_4_single)

rank_1_evaluate= rank_1_count *2 - rank_1_single_count
rank_2_evaluate= rank_2_count *2 - rank_2_single_count
rank_3_evaluate= rank_3_count *2 - rank_3_single_count
rank_4_evaluate= rank_4_count *2 - rank_4_single_count

print()
