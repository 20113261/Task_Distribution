import gevent
import random
import pika_send
import datetime
import math
import time
from rabbitmq.consumer import connect_rabbitmq
from collections import defaultdict
from rabbitmq.mongo_data import receive_mongo_data
from functools import partial
from pika import channel

task_level =\
    {'flight': [0,1,2,3,4],
    'Round': [12, 13, 14],
    'multiflight_task_level': [0],
     'Hotel':[5,6,7,8,100]}


total_task_level_dict = {}
collection_task_level = {}
level_dict = {}
distribute_result = {}


def first_calculate_step(task_type):
    '''
    :return:
    per_level_dict为各个package_id的文档数量，eg:{12: 310724, 13: 747187, 14: 725389}
    total_count是所有package_id的文档总数，eg:1783300
    level_dict是各个package_id分collection集合下的文档数量，eg:{12: {'DateTask_Round_Flight_cleartripRoundFlight_20180221': 50343, 'DateTask_Round_Flight_cleartripRoundFlight_20180210': 0, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180209': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180209': 0, 'DateTask_Round_Flight_pricelineRoundFlight_20180208': 38233, 'DateTask_Round_Flight_ebookersRoundFlight_20180208': 32524, 'DateTask_Round_Flight_cleartripRoundFlight_20180208': 31659, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180221': 14000, 'DateTask_Round_Flight_ebookersRoundFlight_20180209': 0, 'DateTask_Round_Flight_ebookersRoundFlight_20180221': 40102, 'DateTask_Round_Flight_pricelineRoundFlight_20180210': 0, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180210': 0, 'DateTask_Round_Flight_cleartripRoundFlight_20180209': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180208': 29756, 'DateTask_Round_Flight_ebookersRoundFlight_20180210': 0, 'DateTask_Round_Flight_pricelineRoundFlight_20180221': 39161, 'DateTask_Round_Flight_orbitzRoundFlight_20180221': 34946, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180208': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180210': 0, 'DateTask_Round_Flight_pricelineRoundFlight_20180209': 0}, 13: {'DateTask_Round_Flight_cleartripRoundFlight_20180221': 0, 'DateTask_Round_Flight_cleartripRoundFlight_20180210': 52246, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180209': 49478, 'DateTask_Round_Flight_orbitzRoundFlight_20180209': 53630, 'DateTask_Round_Flight_pricelineRoundFlight_20180208': 50343, 'DateTask_Round_Flight_ebookersRoundFlight_20180208': 49997, 'DateTask_Round_Flight_cleartripRoundFlight_20180208': 44634, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180221': 0, 'DateTask_Round_Flight_ebookersRoundFlight_20180209': 58820, 'DateTask_Round_Flight_ebookersRoundFlight_20180221': 0, 'DateTask_Round_Flight_pricelineRoundFlight_20180210': 64183, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180210': 49132, 'DateTask_Round_Flight_cleartripRoundFlight_20180209': 59166, 'DateTask_Round_Flight_orbitzRoundFlight_20180208': 60031, 'DateTask_Round_Flight_ebookersRoundFlight_20180210': 58993, 'DateTask_Round_Flight_pricelineRoundFlight_20180221': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180221': 0, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180208': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180210': 50516, 'DateTask_Round_Flight_pricelineRoundFlight_20180209': 46018}, 14: {'DateTask_Round_Flight_cleartripRoundFlight_20180221': 0, 'DateTask_Round_Flight_cleartripRoundFlight_20180210': 48786, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180209': 58128, 'DateTask_Round_Flight_orbitzRoundFlight_20180209': 52938, 'DateTask_Round_Flight_pricelineRoundFlight_20180208': 56398, 'DateTask_Round_Flight_ebookersRoundFlight_20180208': 50343, 'DateTask_Round_Flight_cleartripRoundFlight_20180208': 46883, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180221': 0, 'DateTask_Round_Flight_ebookersRoundFlight_20180209': 52419, 'DateTask_Round_Flight_ebookersRoundFlight_20180221': 0, 'DateTask_Round_Flight_pricelineRoundFlight_20180210': 49478, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180210': 52246, 'DateTask_Round_Flight_cleartripRoundFlight_20180209': 44115, 'DateTask_Round_Flight_orbitzRoundFlight_20180208': 54841, 'DateTask_Round_Flight_ebookersRoundFlight_20180210': 49478, 'DateTask_Round_Flight_pricelineRoundFlight_20180221': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180221': 0, 'DateTask_Round_Flight_cheapticketsRoundFlight_20180208': 0, 'DateTask_Round_Flight_orbitzRoundFlight_20180210': 58474, 'DateTask_Round_Flight_pricelineRoundFlight_20180209': 50862}}
    '''
    per_level_dict = {}

    total_count = 0
    for i in task_level.get(task_type, ''):
        level = {}
        per_level_count = 0
        for collection_name in pika_send.date_task_db.collection_names():
            type = collection_name.split('_')[1]
            if type in ['Round', 'Hotel']:
                data_count = pika_send.date_task_db[collection_name].find({'package_id':i, 'run':0, 'used_times': {'$lt': 2}, 'finished': 0}).count() #此条件注意
                # collection_task_level[collection_name] = data_count
                level[collection_name] = data_count
                per_level_count += data_count
        total_count += per_level_count
        per_level_dict[i] = per_level_count
        level_dict[i] = level
        print('level_dict', level_dict)
        print('total_count', total_count, 'per_level_dict', per_level_dict)
    # per_level_dict = sorted(per_level_dict.items(), key=lambda d: d[0])
    return total_count, per_level_dict, level_dict


def second_calculate_step(total_count):
    '''
    :param total_count:
    :return: per_share_count为计算出的每五分钟的入队数量
    '''
    now_time = datetime.datetime.now()
    next_day_str = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'
    next_day = datetime.datetime.strptime(next_day_str,"%Y-%m-%d %H:%M:%S")
    # print(next_day)

    # print(datetime.datetime.strptime("2018-02-03 00:00:00","%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'))
    shares = (next_day - datetime.datetime.now()).total_seconds()/60/5
    time_shares = math.floor(shares)
    per_share_count = math.ceil(total_count/time_shares)
    print('---------second_calculate_step---------')
    print(time_shares, shares, per_share_count)

    return 200000 #per_share_count


def third_calculate_step(per_share_count, per_level):
    '''
    根据每五分钟的入队数量和排序后的per_level_dict，计算入队数量中，每个package_id应分配的数量。
    注：增加对昨天入队优先级的考虑时，代码在此函数中添加
    :param per_share_count:
    :param per_level:
    :return: 每个package_id应分配的数量
    '''
    global distribute_result
    i = per_level[0]
    if i[1] >= per_share_count:
        distribute_result[i[0]] = per_share_count
        print('package_id为%d取%d'%(i[0], per_share_count))
        distribute_result[i[0]] = per_share_count
    else:
        print('package_id为%d取%d'%(i[0], i[1]))
        distribute_result[i[0]] = i[1]
        if len(per_level)>1:
            third_calculate_step(per_share_count - i[1], per_level[1:])
    print(per_level[1:])
    return distribute_result


def fourth_calculate_step(distribute_result, per_level_dict, level_dict):
    '''
    计算各个package_id分collection集合下,应分配的文档数量
    :param distribute_result:需分配文档的总数
    :param per_level_dict:各个package_id的文档总数
    :param level_dict:各个package_id分collection集合下的文档数量
    :return:
    '''
    final_level_count = defaultdict(list)

    for key, value in distribute_result.items():
        if value == 0:
            continue
        per_level_count = per_level_dict.get(key, 1)
        mongo_count = level_dict.get(key, {})
        for collection_name, number in mongo_count.items():
            count = math.ceil(number * value / per_level_count)
            final_level_count[collection_name].append((key, count))
            print(collection_name, count, number, value, per_level_count)
    return final_level_count


def final_distribute(task_type):
    total_count, per_level_dict, level_dict = first_calculate_step(task_type)
    per_share_count = second_calculate_step(total_count)
    per_level = sorted(per_level_dict.items(), key=lambda d: d[0])
    distribute_result = third_calculate_step(per_share_count, per_level)  # per_share_count, per_level_dict
    print(distribute_result)

    return fourth_calculate_step(distribute_result, per_level_dict, level_dict)


def insert_mongo_data(queue_name, collection_name, mongo_tuple_list):
    # for collection_name, mongo_tuple_list in final_distribute_result.items():
    if queue_name in collection_name:
        for mongo_tuple in mongo_tuple_list:
            cursor = pika_send.date_task_db[collection_name].find({'package_id': mongo_tuple[0], 'run': 0, 'used_times': {'$lt': 2}, 'finished': 0}).limit(mongo_tuple[1])
            yield from cursor


def update_running(collection_name, tid, value):
    update_time = datetime.datetime.now()
    pika_send.date_task_db[collection_name].update({'tid': tid}, {'$set': {'run': value, 'update_time': update_time}})


if __name__ == '__main__':
    final_distribute = final_distribute('hotel')

    print(final_distribute)


