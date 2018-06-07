#coding:utf-8
# @Time    : 2018/5/2
# @Author  : xiaopeng
# @Site    : boxueshuyuan
# @File    : query_server.py
# @Software: PyCharm
import init_path
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.concurrent
import json
# import memory_profiler
import os
import math
import redis
import datetime
import pymongo
from urllib import parse
from queue import Queue
from rabbitmq import pika_send
from tornado.options import define
from conf import config, task_source
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rabbitmq.consumer import ExampleConsumer
from rabbitmq.producter import final_distribute, insert_mongo_data, update_running
from logger_file import get_logger
from rabbitmq.consumer import connect_rabbitmq, insert_spider_result, feed_back_date_task, slave_take_times, task_temporary_monitor, query_temporary_task
from model.TaskType import TaskType
from common.InsertDateTask import InsertDateTask
from common.InsertBaseTask import InsertBaseTask

port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()

class Executor(ThreadPoolExecutor):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not getattr(cls, '_instance', None):
            cls._instance = ThreadPoolExecutor(max_workers=args[0])
        return cls._instance

def print_(data):
    j = 0
    for i in range(100000000):
        j += 1
    print('取走更新{}'.format(data))

pool = redis.ConnectionPool(host='10.10.180.145', port=6379,  decode_responses=True, db=10)
r = redis.Redis(connection_pool=pool)
r.lpush('dd', '{"a":1}')
r.lpush('dd', '{"b":1}')
print(r.setex('gg', '1', 5))
print(r.setex('gg', '2', 5))

response = []
for i in range(4 - 1):
    content = r.lpop('dd')
    if content:
        response.append(eval(content))

class QueryTask(tornado.web.RequestHandler):
    executor = Executor(5)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        try:
            # self.get_argument('')
            print('来了')
            future = Executor().submit(self.async_get)
            response = yield tornado.gen.with_timeout(datetime.timedelta(minutes=10), future,
                                                      quiet_exceptions=tornado.gen.TimeoutError)
            self.write(response.result())
            if response.result():
                tornado.ioloop.IOLoop.instance().add_timeout(1, callback=partial(print_, response.result()))
                print('write完')
        except Exception as e:
            print('出错了')

    @tornado.concurrent.run_on_executor
    def async_get(self):
        j = 0
        for i in range(100000000):
            j+=1
        return str(j)

application = tornado.web.Application([
    (r'/workload', QueryTask),
    # (r'/admin', control.AdminHandler)
])

def async_get(date_list, rank):
    mongo_host = 'mongodb://root:miaoji1109-=@10.19.2.103:27017/'
    clinet = pymongo.MongoClient(host=mongo_host)
    coll = clinet['case_result']['20180502']
    coll_base = clinet['RoutineBaseTask']['BaseTask_Hotel']
    result = []

    cursor = coll.find({'collection_name': {'$regex': 'Hotel'}, 'error': {'$ne': 0}})
    cursor_city_list = coll_base.find({'package_id': rank})
    city_list = []
    for i in cursor_city_list:
        city_list.append(i['task_args']['city_id'])

    for i in cursor:
        source = i['source']
        city = i['content'].split('&')[0]
        content = i['content']
        checkin_date = i['content'].split('&')[-1]
        if city not in city_list:
            continue
        if checkin_date not in date_list:
            continue
        result.append([source, city, content])
    return result

# res = async_get(['20180601'], 5)
print()

if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(123456, '0.0.0.0')
    http_server.start()
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()
