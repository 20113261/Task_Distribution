#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午2:58
# @Author  : Hou Rong
# @Site    : 
# @File    : generate_task_info.py
# @Software: PyCharm
from conn_pool import base_data_pool
from mysql_execute import fetchall
from logger import get_logger

logger = get_logger("generate_base_task")

flight_source = ['orbitzFlight', 'ebookersFlight', 'cheapticketsFlight', 'expediaFlight', 'travelocityFlight',
                 'tripstaFlight', 'pricelineFlight']

flightSource = {'orbitzFlight': 'ebookersFlight', 'ebookersFlight': 'cheapticketsFlight',
                'cheapticketsFlight': 'expediaFlight', 'expediaFlight': 'travelocityFlight',
                'travelocityFlight': 'ctripFlightRoutine', 'ctripFlightRoutine': 'tripstaFlight',
                'tripstaFlight': 'pricelineFlight', 'pricelineFlight': 'orbitzFlight'}

roundflight_source = ['cleartripRoundFlight', 'cheapticketsRoundFlight', 'orbitzRoundFlight', 'ebookersRoundFlight',
                      'pricelineRoundFlight']

roundflightsource = {'cleartripRoundFlight': 'cheapticketsRoundFlight', 'cheapticketsRoundFlight': 'orbitzRoundFlight',
                     'orbitzRoundFlight': 'ebookersRoundFlight', 'ebookersRoundFlight': 'pricelineRoundFlight',
                     'pricelineRoundFlight': 'cleartripRoundFlight'}

train_source = ['ctripRail']
bus_source = ['megabusUSBus']

hotel_sopurce = ['expediaListHotel', 'hotelsListHotel', 'bookingListHotel', 'elongListHotel', 'agodaListHotel',
                 'ctripTWHotel']

multiflight_source = ['cleartripMultiFlight', 'orbitzMultiFlight', 'travelocityMultiFlight', 'cheapticketsMultiFlight',
                      'expediaMultiFlight']

type_data = {
    'flight': flight_source,
    'roundflight': roundflight_source,
    'hotel': hotel_sopurce,
    'multiflight': multiflight_source,
    'bus': bus_source,
    'train': train_source
}


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

    rank1_sql = '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('北京', '上海', '广州', '深圳', '香港');'''

    rank2_sql = '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM airport, city, country
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.name IN ('成都', '武汉', '昆明', '郑州', '厦门', '南京', '重庆', '青岛', '长沙', '杭州', '天津', '大连', '西安', '长春');'''

    rank3_sql = '''SELECT
  airport.iata_code,
  airport.inner_order,
  city.id,
  country.continent_id,
  city.name,
  city.tri_code
FROM country, airport, city
WHERE (city.id = airport.belong_city_id) AND airport.status = 'Open' AND city.country_id = country.mid AND
      city.trans_degree = 0 AND (city.status_online = 'Open' OR city.status_test = 'Open') AND city.country_id <> '101'
      AND city.name <> '香港';'''

    rank4_sql = '''SELECT
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

    rank5_sql = '''SELECT
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

    rank1_items = list(rank1.items())
    rank2_items = list(rank2.items())
    rank3_items = list(rank3.items())
    rank4_items = list(rank4.items())
    rank5_items = list(rank5.items())
    rank6_items = list(rank6.items())
    data = []
    write_data = []
    judge_data = []
    for r1_item in rank1_items:
        for r2_item in rank3_items:
            static_i += 1
            if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                data.append(
                    [r1_item[1]['iata_code'], r2_item[1]['iata_code'], 0, flight_source[static_i % len(flight_source)]])
                judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 0))
            if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                data.append(
                    [r2_item[1]['iata_code'], r1_item[1]['iata_code'], 0, flight_source[static_i % len(flight_source)]])
                judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 0))

    logger.info("[rank 1-3 finished]")

    for r1_item in rank1_items:
        for r2_item in rank4_items:
            static_i += 1
            if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                data.append(
                    [r1_item[1]['iata_code'], r2_item[1]['iata_code'], 1, flight_source[static_i % len(flight_source)]])
                write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 1))
            if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                data.append(
                    [r2_item[1]['iata_code'], r1_item[1]['iata_code'], 1, flight_source[static_i % len(flight_source)]])
                write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 1))

    logger.info("[rank 1-4 finished]")

    for r1_item in rank2_items:
        for r2_item in rank3_items:
            static_i += 1
            if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                data.append(
                    [r1_item[1]['iata_code'], r2_item[1]['iata_code'], 1, flight_source[static_i % len(flight_source)]])
                write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 1))
            if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                data.append(
                    [r2_item[1]['iata_code'], r1_item[1]['iata_code'], 1, flight_source[static_i % len(flight_source)]])
                write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 1))

    logger.info("[rank 2-3 finished]")

    for r1_item in rank3_items:
        for r2_item in rank3_items:
            static_i += 1
            if r1_item[1]['city_id'] != r2_item[1]['city_id'] and r2_item[1]['continent_id'] == r1_item[1][
                'continent_id'] and r1_item[1]['iata_code'] != r2_item[1]['iata_code']:
                if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 2,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 2))

    logger.info("[rank 3-3 finished]")

    for r1_item in rank4_items:
        for r2_item in rank3_items:
            static_i += 1
            if r2_item[1]['continent_id'] == r1_item[1]['continent_id'] and r1_item[1]['iata_code'] != r2_item[1][
                'iata_code']:
                if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 2,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 2))
                if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                    data.append([r2_item[1]['iata_code'], r1_item[1]['iata_code'], 2,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 2))

    logger.info("[rank 4-3 finished]")

    for r1_item in rank4_items:
        for r2_item in rank4_items:
            static_i += 1
            if r1_item[1]['city_id'] != r2_item[1]['city_id'] and r2_item[1]['continent_id'] == r1_item[1][
                'continent_id'] and r1_item[1]['iata_code'] != r2_item[1]['iata_code']:
                if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 3,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 3))

    logger.info("[rank 4-4 finished]")

    for r1_item in rank2_items:
        for r2_item in rank4_items:
            static_i += 1
            if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                data.append(
                    [r1_item[1]['iata_code'], r2_item[1]['iata_code'], 3, flight_source[static_i % len(flight_source)]])
                write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 3))

    logger.info("[rank 2-4 finished]")

    for r1_item in rank2_items:
        for r2_item in rank4_items:
            if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                data.append(
                    [r2_item[1]['iata_code'], r1_item[1]['iata_code'], 3, flight_source[static_i % len(flight_source)]])
                write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 3))

    logger.info("[rank 2-4 finished]")

    for r1_item in rank6_items:
        for r2_item in rank3_items:
            static_i += 1
            if r2_item[1]['continent_id'] == r1_item[1]['continent_id'] and r1_item[1]['iata_code'] != r2_item[1][
                'iata_code']:
                if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 4, 'tripstaFlight'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 4, 'pricelineFlight'])
                    write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 4))
                if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                    data.append([r2_item[1]['iata_code'], r1_item[1]['iata_code'], 4, 'tripstaFlight'])
                    data.append([r2_item[1]['iata_code'], r1_item[1]['iata_code'], 4, 'pricelineFlight'])
                    write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 4))

    logger.info("[rank 6-3 finished]")

    for r1_item in rank5_items:
        for r2_item in rank3_items:
            static_i += 1
            if r2_item[1]['continent_id'] == r1_item[1]['continent_id'] and r1_item[1]['iata_code'] != r2_item[1][
                'iata_code']:
                if r1_item[1]['iata_code'] + r2_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r1_item[1]['iata_code'] + r2_item[1]['iata_code'])
                    data.append([r1_item[1]['iata_code'], r2_item[1]['iata_code'], 4,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r1_item[1]['city_id'], r2_item[1]['city_id'], 4))
                if r2_item[1]['iata_code'] + r1_item[1]['iata_code'] not in judge_data:
                    judge_data.append(r2_item[1]['iata_code'] + r1_item[1]['iata_code'])
                    data.append([r2_item[1]['iata_code'], r1_item[1]['iata_code'], 4,
                                 flight_source[static_i % len(flight_source)]])
                    write_data.append((r2_item[1]['city_id'], r1_item[1]['city_id'], 4))

    logger.info("[rank 6-3 finished]")

    for each in data:
        yield each


if __name__ == '__main__':
    generate_flight_base_task_info()
