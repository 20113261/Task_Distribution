#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午2:58
# @Author  : Hou Rong
# @Site    : 
# @File    : generate_task_info.py
# @Software: PyCharm
import traceback
from conn_pool import base_data_pool, source_info_pool, task_db_monitor_db_pool
from mysql_execute import fetchall, fetchall_ss
from logger_file import get_logger
from common.sql_query import  flight_base_task_query, temporary_flight_base_task_query, roundflight_base_task_query,\
    hotel_base_task_query,multiflight_base_task_query,rail_base_task_query,ferries_base_task_query

logger = get_logger("generate_base_task")


def get_rank_result(sql):
    rank = {}
    rank_list = [
        (rank, sql),
    ]
    for each_rank, each_rank_sql in rank_list:
        for row in fetchall(base_data_pool, each_rank_sql):
            iata_code = row[0]
            inner_order = row[1]
            city_id = row[2]
            continent_id = row[3]
            city_name = row[4]
            tri_code = row[5]
            if city_id in each_rank:
                if int(inner_order) > 0:
                    if int(each_rank[city_id]['inner_order']) < 0 or int(inner_order) < int(
                            each_rank[city_id]['inner_order']):
                        each_rank[city_id]['iata_code'] = iata_code
                        each_rank[city_id]['inner_order'] = inner_order
            else:
                each_rank[city_id] = {}
                each_rank[city_id]['iata_code'] = iata_code
                each_rank[city_id]['inner_order'] = inner_order
                each_rank[city_id]['continent_id'] = continent_id
                each_rank[city_id]['city_name'] = city_name
                each_rank[city_id]['tri_code'] = tri_code
                each_rank[city_id]['city_id'] = city_id
    return each_rank


def generate_temporary_flight_task(temporary_city:str):
    # temporary_sql = temporary_flight_base_task_query['temporary_sql']
    # temporary_city = ''
    # for row in fetchall(base_data_pool, temporary_sql):
    #     city = "'{}',".format(row[0])
    #     temporary_city += city
    # temporary_city = temporary_city.rstrip(',')
    if temporary_city is None:
        return []
    temporary_city = temporary_city.lstrip('[')
    temporary_city = temporary_city.rstrip(']')
    temp_rank3 = get_rank_result(temporary_flight_base_task_query['rank3_sql'].format(temporary_city))
    temp_rank4 = get_rank_result(temporary_flight_base_task_query['rank4_sql'].format(temporary_city))
    temp_rank5 = get_rank_result(temporary_flight_base_task_query['rank5_sql'].format(temporary_city))

    rank1_sql = flight_base_task_query['rank1_sql']
    rank2_sql = flight_base_task_query['rank2_sql']
    rank3_sql = flight_base_task_query['rank3_sql']
    rank4_sql = flight_base_task_query['rank4_sql']
    rank5_sql = flight_base_task_query['rank5_sql']
    rank1 = get_rank_result(rank1_sql)
    rank2 = get_rank_result(rank2_sql)
    rank3 = get_rank_result(rank3_sql)
    rank4 = get_rank_result(rank4_sql)
    rank5 = get_rank_result(rank5_sql)

    data = []
    judge_data = set()

    target_data = [
        # rank0 1-3
        ("1-3", rank1, temp_rank3, False),

        # rank1 1-4, 2-3
        ("1-4", rank1, temp_rank4, False),
        ("2-3", rank2, temp_rank3, False),

        # rank2 3-3, 3-4
        ("3-3", rank3, temp_rank3, True),
        ("3-4", rank3, temp_rank4, True),

        # rank3 2-4, 4-4
        ("2-4", rank2, temp_rank4, True),
        ("4-4", rank4, temp_rank4, True),

        # rank4 3-5，以及到部分国内城市
        ("3-5", rank3, temp_rank5, True)
    ]

    package_id = -1

    for report_key, first_rank, last_rank, need_continent_filter in target_data:
        for v1 in first_rank.values():
            for v2 in last_rank.values():
                if v1['iata_code'] == v2['iata_code']:
                    # iata_code 相同时过滤
                    continue

                if need_continent_filter and v1['continent_id'] != v2['continent_id']:
                    # 当需要大洲筛选时，v1 v2 不在一个大洲内
                    continue

                # 去程
                if (v1['iata_code'], v2['iata_code']) not in judge_data:
                    judge_data.add((v1['iata_code'], v2['iata_code']))
                    data.append(
                        [
                            v1['iata_code'],
                            v2['iata_code'],
                            package_id,
                            # flight_source[static_i % len(flight_source)]
                        ]
                    )

                # 返程
                if (v2['iata_code'], v1['iata_code']) not in judge_data:
                    judge_data.add((v2['iata_code'], v1['iata_code']))
                    data.append(
                        [
                            v2['iata_code'],
                            v1['iata_code'],
                            package_id,
                            # flight_source[static_i % len(flight_source)]
                        ]
                    )

        logger.info("[package_id: {}][rank {} finished]".format(package_id, report_key))

    yield from data

def generate_flight_base_task_info():
    l_chine_city = ["40051", "51517", "51518", "20103", "20104", "20107", "20108", "20111", "20112", "20115", "20116",
                    "20118", "20119", "20121", "20122", "20123", "20124", "20125", "20126", "20129", "20130", "20133",
                    "20134", "20271", "20284", "20287", "21381", "21483", "21484", "21485", "21486", "21487", "21488",
                    "21489", "21490", "21491", "21492", "21493", "21494", "21495", "21496", "21497", "21498", "21499",
                    "21500", "21501", "21502", "21503", "21504", "21505", "21506", "21507", "21508", "21509", "21510",
                    "21511", "21512", "21513", "21514", "21515", "21516", "21517", "21518", "21519", "21520", "21521",
                    "21522", "21523", "21524", "21525", "21526", "21527", "21528", "21529", "21530", "21531", "21532",
                    "21533", "21534", "21535", "21536", "20371"]

    static_i = 0

    rank1_sql = flight_base_task_query['rank1_sql']
    rank2_sql = flight_base_task_query['rank2_sql']
    rank3_sql = flight_base_task_query['rank3_sql']
    rank4_sql = flight_base_task_query['rank4_sql']
    rank5_sql = flight_base_task_query['rank5_sql']

    rank6_sql = '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = -1 AND city.id IN ({});'''.format(','.join(map(lambda x: "'{}'".format(x), l_chine_city)))

    rank1 = {}
    rank2 = {}
    rank3 = {}
    rank4 = {}
    rank5 = {}
    rank6 = {}

    rank_list = [
        (rank1, rank1_sql),
        (rank2, rank2_sql),
        (rank3, rank3_sql),
        (rank4, rank4_sql),
        (rank5, rank5_sql),
        (rank6, rank6_sql)
    ]

    for each_rank, each_rank_sql in rank_list:
        for row in fetchall(base_data_pool, each_rank_sql):
            iata_code = row[0]
            inner_order = row[1]
            city_id = row[2]
            continent_id = row[3]
            city_name = row[4]
            tri_code = row[5]
            if city_id in each_rank:
                if int(inner_order) > 0:
                    if int(each_rank[city_id]['inner_order']) < 0 or int(inner_order) < int(
                            each_rank[city_id]['inner_order']):
                        each_rank[city_id]['iata_code'] = iata_code
                        each_rank[city_id]['inner_order'] = inner_order
            else:
                each_rank[city_id] = {}
                each_rank[city_id]['iata_code'] = iata_code
                each_rank[city_id]['inner_order'] = inner_order
                each_rank[city_id]['continent_id'] = continent_id
                each_rank[city_id]['city_name'] = city_name
                each_rank[city_id]['tri_code'] = tri_code
                each_rank[city_id]['city_id'] = city_id

    logger.info("[rank dict finished]")

    data = []
    judge_data = set()

    target_data = [
        # rank0 1-3
        ("1-3", rank1, rank3, 0, False),

        # rank1 1-4, 2-3
        ("1-4", rank1, rank4, 1, False),
        ("2-3", rank2, rank3, 1, False),

        # rank2 3-3, 3-4
        ("3-3", rank3, rank3, 2, True),
        ("3-4", rank3, rank4, 2, True),

        # rank3 2-4, 4-4
        ("2-4", rank2, rank4, 3, True),
        ("4-4", rank4, rank4, 3, True),

        # rank4 3-5，以及到部分国内城市
        ("3-5", rank3, rank5, 4, True),
        ("3-6", rank3, rank6, 4, True),
    ]

    logger.info(str(len(rank1)) + ',' + str(len(rank2)) + ',' + str(len(rank3)) + ',' + str(len(rank4)) + ',' + str(len(rank5)) + ',' + str(len(rank6)))

    for report_key, first_rank, last_rank, package_id, need_continent_filter in target_data:
        for v1 in first_rank.values():
            for v2 in last_rank.values():
                if v1['iata_code'] == v2['iata_code']:
                    # iata_code 相同时过滤
                    continue

                if need_continent_filter and v1['continent_id'] != v2['continent_id']:
                    # 当需要大洲筛选时，v1 v2 不在一个大洲内
                    continue

                static_i += 1
                # 去程
                if (v1['iata_code'], v2['iata_code']) not in judge_data:
                    judge_data.add((v1['iata_code'], v2['iata_code']))
                    data.append(
                        [
                            v1['iata_code'],
                            v2['iata_code'],
                            package_id,
                            # flight_source[static_i % len(flight_source)]
                        ]
                    )

                # 返程
                if (v2['iata_code'], v1['iata_code']) not in judge_data:
                    judge_data.add((v2['iata_code'], v1['iata_code']))
                    data.append(
                        [
                            v2['iata_code'],
                            v1['iata_code'],
                            package_id,
                            # flight_source[static_i % len(flight_source)]
                        ]
                    )


        logger.info("[package_id: {}][rank {} finished]".format(package_id, report_key))
    logger.info('data:' + str(len(data)))
    yield from data


def generate_round_flight_base_task_info():
    static_i = 0

    rank1_sql = roundflight_base_task_query['rank1_sql']
    rank2_sql = roundflight_base_task_query['rank2_sql']
    rank3_sql = roundflight_base_task_query['rank3_sql']
    rank4_sql = roundflight_base_task_query['rank4_sql']

    rank1 = {}
    rank2 = {}
    rank3 = {}
    rank4 = {}

    rank_list = [
        (rank1, rank1_sql),
        (rank2, rank2_sql),
        (rank3, rank3_sql),
        (rank4, rank4_sql),
    ]

    for each_rank, each_rank_sql in rank_list:
        for row in fetchall(base_data_pool, each_rank_sql):
            iata_code = row[0]
            inner_order = row[1]
            city_id = row[2]
            continent_id = row[3]
            city_name = row[4]
            tri_code = row[5]
            if city_id in each_rank:
                if int(inner_order) > 0:
                    if int(each_rank[city_id]['inner_order']) < 0 or int(inner_order) < int(
                            each_rank[city_id]['inner_order']):
                        each_rank[city_id]['iata_code'] = iata_code
                        each_rank[city_id]['inner_order'] = inner_order
            else:
                each_rank[city_id] = {}
                each_rank[city_id]['iata_code'] = iata_code
                each_rank[city_id]['inner_order'] = inner_order
                each_rank[city_id]['continent_id'] = continent_id
                each_rank[city_id]['city_name'] = city_name
                each_rank[city_id]['tri_code'] = tri_code
                each_rank[city_id]['city_id'] = city_id

    logger.info("[rank dict finished]")

    target_data = [
        # rank0 1-3
        ("1-3", rank1, rank3, 12),

        # rank1 1-4, 2-3
        ("1-4", rank1, rank4, 13),
        ("2-3", rank2, rank3, 13),

        # rank2 3-3, 3-4
        ("2-4", rank2, rank4, 14),
    ]

    data = []
    judge_data = set()

    for report_key, first_rank, last_rank, package_id in target_data:
        logger.info(report_key)
        for v1 in first_rank.values():
            for v2 in last_rank.values():
                logger.info(v2['iata_code'])
                if v1['iata_code'] == v2['iata_code']:
                    # iata_code 相同时过滤
                    continue
                static_i += 1

                if (v1['iata_code'], v2['iata_code']) not in judge_data:
                    judge_data.add((v1['iata_code'], v2['iata_code']))
                    data.append(
                        [
                            v1['iata_code'],
                            v2['iata_code'],
                            package_id,
                            # roundflight_source[static_i % len(roundflight_source)],
                            v2["continent_id"]
                        ]
                    )


        logger.info("[package_id: {}][rank {} finished]".format(package_id, report_key))

    yield from data


def generate_multi_flight_base_task_info():
    northeast_asia = [20071, 20070, 20072, 20073, 20166, 20181, 20056, 20057, 20220]
    southeast_asia = [20087, 20082, 20083, 20080, 20064, 20216, 20065, 20096, 20098]
    taiwan = [20078,20194,20079]
    europe = [10009,	10001,	10008,	10013,	10007,	10005,	10002,	10028,	10019,	10006,	10044,	10026,	10014,	10038,	10012,	10053,	10015,	10010,	10004,	10017,	10020,	10047,	10605,	10621,	10011,	10025,	10051,	10043,	10092,	10027,	10011,	11095,	10071]
    north_ameriaca = [50002,	50003,	50012,	50010,	50016,	50017,	50014,	50004,	50026,	50013,	50036,	50019,	50001,	50005,	50047]
    oceania = [30010,	30042,	30091,	30018,	30030,	30099,	30095,	30032]

    task_result = []
    task_northeast_asia = multi_flight_join_means(northeast_asia)
    task_southeast_asia = multi_flight_join_means(southeast_asia)
    task_taiwan = multi_flight_join_means(taiwan)
    task_europe = multi_flight_join_means(europe)
    task_north_ameriaca = multi_flight_join_means(north_ameriaca)
    task_oceania = multi_flight_join_means(oceania)

    task_result.extend(task_northeast_asia)
    task_result.extend(task_southeast_asia)
    task_result.extend(task_taiwan)
    task_result.extend(task_europe)
    task_result.extend(task_north_ameriaca)
    task_result.extend(task_oceania)

    yield from task_result
    print('')

def multi_flight_join_means(city_id_list:list):
    city_id_list = str([str(id) for id in city_id_list ])
    city_id_list = city_id_list.rstrip(']').lstrip('[')

    sql = multiflight_base_task_query['dest_city_sql'].format(city_id_list)

    rank = get_rank_result(sql)

    logger.info("[rank dict finished]")
    task_list = []
    airport_info_list = rank.values()
    for airport_info_1 in airport_info_list:
        for airport_info_2 in airport_info_list:
            if airport_info_1.__ne__(airport_info_2):
                task_list.append([airport_info_1['iata_code'], airport_info_2['iata_code'], 15, airport_info_1['continent_id']])
    return task_list


def get_mutli_dept_airport():
    sql = multiflight_base_task_query['dept_city_sql']

    rank = get_rank_result(sql)
    return [airport['iata_code'] for airport in rank.values()]



def generate_hotel_base_task_info(temporary_city_str=None):

#     sql = '''SELECT
#       city.id AS city_id,
#       city.trans_degree,
#       city.grade,
#       ota_location.source,
#       ota_location.suggest,
#       ota_location.suggest_type,
#       ota_location.country_id
#     FROM ota_location
#       LEFT JOIN city ON ota_location.city_id= city.id
#     WHERE source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip');'''

    if temporary_city_str is None:
        sql = hotel_base_task_query['base_sql']
        #ota_location中无city_id的数量为611459,有city_id的有28889。总共640348. package_id:5-1654,6-2477,7-14690,8-10068,100-611459
        for row in fetchall_ss(source_info_pool, sql):
            city_id, suggest, suggest_type, package_id, country_id, source = get_hotel_info(row)
            yield city_id, suggest, suggest_type, package_id, country_id, source
    else:
        temporary_city_str = temporary_city_str.rstrip(']').lstrip('[')
        sql = hotel_base_task_query['temp_sql'].format(temporary_city_str)

        for row in fetchall_ss(source_info_pool, sql):
            city_id, suggest, suggest_type, package_id, country_id, source = get_hotel_info(row)
            yield city_id, suggest, suggest_type, -1, country_id, source


def get_hotel_info(row):
    city_id = row[0]
    trans_degree = row[1]
    grade = row[2]
    source = row[3]
    source += "ListHotel"

    suggest = row[4]
    suggest_type = row[5]
    country_id = 'null'
    # country_id = row[6]
    # package_id = 100

    if city_id is None:
        package_id = 100
    elif trans_degree == 0 or trans_degree == 1 or grade == 1 or grade == 2:
        package_id = 5
    elif grade == 3 or grade == 4:
        package_id = 6
    elif grade >= 5:
        package_id = 7
    else:
        package_id = 8
    return city_id, suggest, suggest_type, package_id, country_id, source

# todo rail 的内容可能需要修改，当前依赖 ctrip_rail_table 发的任务的
def generate_rail_base_task_info():
    static_rail_task = ['10001_10009', '10009_10001', '10001_10015', '10015_10001', '10001_10007', '10007_10001',
                        '10001_10006', '10006_10001', '10001_10005', '10005_10001', '10001_10017', '10017_10001',
                        '10001_10128', '10128_10001', '10001_10013', '10013_10001', '10001_10012', '10012_10001',
                        '10001_10008', '10008_10001', '10005_10019', '10019_10005', '10005_10068', '10068_10005',
                        '10005_10011', '10011_10005', '10005_10016', '10016_10005', '10005_10006', '10006_10005',
                        '10005_10048', '10048_10005', '10003_10010', '10010_10003', '10003_10008', '10008_10003',
                        '10003_10011', '10011_10003', '10020_10006', '10006_10020', '10020_10048', '10048_10020',
                        '10019_10008', '10008_10019', '10019_10057', '10057_10019', '10010_10027', '10027_10010',
                        '10010_10008', '10008_10010', '10010_10011', '10011_10010', '10073_10011', '10011_10073',
                        '10008_10024', '10024_10008', '10063_10042', '10042_10063', '10063_10007', '10007_10063',
                        '10104_10051', '10051_10104', '10025_10051', '10051_10025', '10017_10043', '10043_10017',
                        '10145_10006', '10006_10145', '10009_10015', '10015_10009']
    city_data = {}
    base_sql = rail_base_task_query['base_sql']
    code_sql = rail_base_task_query['rail_code_query']
    for row in fetchall(task_db_monitor_db_pool, base_sql):
        city_id = row[0]
        country_id = row[1]
        city_name = row[2]
        city_data[city_id] = {}
        city_data[city_id]['city_id'] = city_id
        city_data[city_id]['country_id'] = country_id
        city_data[city_id]['rail_code'] = city_name
    items = list(city_data.items())
    data = []
    city_code = {}
    city_id_set = set()
    for item1 in items:
        for item2 in items:
            if item1[1]['city_id'] != item2[1]['city_id'] and item1[1]['country_id'] == item2[1]['country_id']:
                data.append([item1[1]['city_id'], item1[1]['rail_code'], item2[1]['city_id'], item2[1]['rail_code'], 10, "Rail"])
    for task in static_rail_task:
        city_id_set.add(str(task.split('_')[0]))
        city_id_set.add(str(task.split('_')[1]))
    city_id_set_str = str(city_id_set).lstrip('{')
    city_id_set_str = str(city_id_set_str).rstrip('}')
    code_sql = code_sql.format(city_id_set_str)
    for row in fetchall(task_db_monitor_db_pool, code_sql):
        city_code[str(row[0])] = row[1]
    for task in static_rail_task:
        dept_city_code = task.split('_')[0]
        dest_city_code = task.split('_')[1]
        data.append([dept_city_code, city_code[dept_city_code], dest_city_code, city_code[dest_city_code], 10, 'Rail'])

    yield from data


def generate_ferries_base_task_info():
    base_sql = ferries_base_task_query['base_sql']
    data = []
    for row in fetchall(source_info_pool, base_sql):
        route_id = row[0]
        data.append([route_id, 60])
    yield from data


if __name__ == '__main__':

    # for line in generate_round_flight_base_task_info():
    #     print(line)
    '''
    HotelTasks
    '''
    # for i in generate_flight_base_task_info():
    #     pass
    # generate_multi_flight_base_task_info()
    # generate_flight_base_task_info()
    generate_rail_base_task_info()