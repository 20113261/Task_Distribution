import requests, json, random
import time
from bson.objectid import ObjectId
from bson import json_util
from logger_file import get_logger
import traceback
import pymongo
import random
from conf import config

# logger = get_logger('test')
#
# try:
#     logger.info('start')
#     a = 2/0
# except Exception as e:
#     logger.info('出错')
#     # logger.info(traceback.format_exc())
#     logger.exception(msg='出错', exc_info=e)






client = pymongo.MongoClient(host=config.mongo_host)
coll = client['RoutineBaseTask']['BaseTask_Hotel']

def query_hotel_base(city_id):
    index_list = []
    query_count = coll.count({'task_args.city_id': city_id})
    choice_list = [i for i in range(query_count)]
    index_1 = random.choice(choice_list)
    index_list.append(index_1)
    choice_list.remove(index_1)
    if choice_list:
        index_list.append(random.choice(choice_list))
    i = 0
    line_list = []
    for line in coll.find({'task_args.city_id': city_id}):
        if i in index_list:
            yield line
        i += 1
# for i in query_hotel_base('60670'):
#     print(i)

# j = 0
# for line in coll.distinct('task_args.city_id',{'package_id':6}):
#     j += 1
# print(j)


def get_source_task_count_info(source_name):
    '''
    查询指定源下的城市任务，分任务数和package_id的统计情况
    :param source_name:
    :return:
    '''
    for task_len in range(1,7):
        print(task_len, '*'*10, ':')
        for package_id in range(5,9):
            cursor_1 = coll.find({'package_id':package_id, 'task_args.source':source_name})
            city_list = []
            for i in cursor_1:
                city_list.append(i['task_args']['city_id'])

            cursor = coll.aggregate(

                 [
                    {'$match': {'package_id':package_id}},
                  { '$group' : { '_id' : "$task_args.city_id", 'task': { '$push': "$$ROOT"} } }]

            )


            j = 0
            for i in cursor:
                if i['task'][0]['task_args']['city_id'] not in city_list:
                    continue
                if len(i['task']) == task_len:
                    j += 1
            # else:
            #     j += 2
            # print(i)
            print(j)


# get_source_task_count_info('bookingListHotel')


# cursor = coll.count({'task_args.source':'expediaListHotel', 'package_id':{'$ne':100}})

print()

def query_per_package_id_count(package_id):
    cursor = coll.aggregate(

         [{'$match': {'package_id':package_id}},
            { '$group' : { '_id' :"$task_args.city_id", 'task': { '$push': "$$ROOT"} } }]
    )

    j = 0
    for i in cursor:
        if len(i['task'])==1:
            j += 1
        else:
            j += 2
    #     print(i)
    print(j)


def get_agoda_elong_info():
    '''
    查询当城市任务符合以下条件时，该城市的列表页条数在各package_id的分布数量：
        1、该城市同时拥有agoda和elong源任务，
        2、该城市的任务数是3个时，
    :return:count_dict
    '''
    count_dict = {}
    for task_len in range(3,4):
        print(task_len, '*'*10, ':')
        for package_id in range(5,9):
            cursor_1 = coll.find({'package_id':package_id, '$or':[{'task_args.source':'elongListHotel'},{'task_args.source':'agodaListHotel'}]})
            city_list = []
            for i in cursor_1:
                city_list.append(i['task_args']['city_id'])

            cursor = coll.aggregate(

                 [
                    {'$match': {'package_id':package_id}},
                  { '$group' : { '_id' : "$task_args.city_id", 'task': { '$push': "$$ROOT"} } }]

            )

            total = 0
            city = set()
            for i in cursor:
                j = 0
                if i['task'][0]['task_args']['city_id'] not in city_list:
                    continue
                if len(i['task']) == task_len:
                    for line in i['task']:
                        if line['task_args']['source'] == 'agodaListHotel':
                            j += 1
                        if line['task_args']['source'] == 'elongListHotel':
                            j += 1
                        if j == 2:
                            city.add(i['task'][0]['task_args']['city_id'])


            count_dict[package_id] = city
    print()

get_agoda_elong_info()

