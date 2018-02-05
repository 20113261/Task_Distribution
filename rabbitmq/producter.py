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
    'multiflight_task_level': [0]}


total_task_level_dict = {}
collection_task_level = {}
level_dict = {}
distribute_result = {}


def first_calculate_step():
    per_level_dict = {}

    total_count = 0
    for i in task_level.get('Round', ''):
        level = {}
        per_level_count = 0
        for collection_name in pika_send.date_task_db.collection_names():
            type = collection_name.split('_')[1]
            if type == 'Round':
                data_count = pika_send.date_task_db[collection_name].find({'package_id':i, 'run':0}).count()
                # collection_task_level[collection_name] = data_count
                level[collection_name] = data_count
                per_level_count += data_count
        total_count += per_level_count
        per_level_dict[i] = per_level_count
        level_dict[i] = level
        print(level_dict)
        print(total_count, per_level_dict)
    # per_level_dict = sorted(per_level_dict.items(), key=lambda d: d[0])
    return total_count, per_level_dict, level_dict


def second_calculate_step(total_count):
    now_time = datetime.datetime.now()
    next_day_str = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'
    next_day = datetime.datetime.strptime(next_day_str,"%Y-%m-%d %H:%M:%S")
    print(next_day)

    # print(datetime.datetime.strptime("2018-02-03 00:00:00","%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'))
    shares = (next_day - datetime.datetime.now()).total_seconds()/60/5
    time_shares = math.ceil(shares)
    per_share_count = math.ceil(total_count/time_shares)
    print(time_shares, shares, per_share_count)

    return per_share_count


def third_calculate_step(per_share_count, per_level):
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


def final_distribute():
    total_count, per_level_dict, level_dict = first_calculate_step()
    per_share_count = second_calculate_step(total_count)
    per_level = sorted(per_level_dict.items(), key=lambda d: d[0])
    distribute_result = third_calculate_step(per_share_count, per_level)  # per_share_count, per_level_dict
    print(distribute_result)

    return fourth_calculate_step(distribute_result, per_level_dict, level_dict)


def insert_mongo_data(queue_name, collection_name, mongo_tuple_list):
    # for collection_name, mongo_tuple_list in final_distribute_result.items():
    if queue_name in collection_name:
        for mongo_tuple in mongo_tuple_list:
            cursor = pika_send.date_task_db[collection_name].find({'package_id': mongo_tuple[0], 'run': 0}).limit(mongo_tuple[1])
            yield from cursor


def update_running(collection_name, tid, value):
    pika_send.date_task_db[collection_name].update({'tid': tid}, {'$set': {'run': value}})


if __name__ == '__main__':
    final_distribute = final_distribute()

    print(final_distribute)

