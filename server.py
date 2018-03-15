#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:01
# @Author  : Hou Rong
# @Site    : 
# @File    : server.py
# @Software: PyCharm
# !/usr/bin/python
# coding=utf-8
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
from rabbitmq.consumer import connect_rabbitmq, insert_spider_result, feed_back_date_task, slave_take_times
from model.TaskType import TaskType

logger = get_logger('server')
logger.info('aa')
port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()

class Executor(ThreadPoolExecutor):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not getattr(cls, '_instance', None):
            cls._instance = ThreadPoolExecutor(max_workers=30)
        return cls._instance

class GetTask(tornado.web.RequestHandler):
    executor = Executor()

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        # return
        self.data_type = self.get_argument('data_type', '').strip()
        request_count = int(self.get_argument('count', '0').strip())
        self.response = []
        if self.data_type:
            future = Executor().submit(self.async_get, request_count)
            response = yield tornado.gen.with_timeout(datetime.timedelta(10), future,
                                                      quiet_exceptions=tornado.gen.TimeoutError)
            # if response:
            self.write(str(response.result()))
            if len(response.result()) > 0:
                tornado.ioloop.IOLoop.instance().add_timeout(1, callback=partial(slave_take_times, response.result()))
                logger.info('finish this request！{}{}个数据***************'.format(self.data_type, len(response.result())))

    @tornado.concurrent.run_on_executor
    def async_get(self, request_count):
        source_count_dict = {}
        if self.data_type == 'ListHotel':
            source_list = task_source.hotel_source
        elif self.data_type == 'RoundFlight':
            source_list = task_source.round_flight_source
        elif self.data_type == 'Flight':
            source_list = task_source.flight_source
        else:
            return []
        for source in source_list:
            try:
                redis_count = r.llen(source)
            except Exception as e:
                logger.error("发生异常", exc_info=1)
                redis_count = 0
            if redis_count:
                source_count_dict[source] = redis_count
        if len(source_count_dict) == 0:
            return []
        request_count = math.ceil(float(request_count / len(source_count_dict)))
        for source, redis_count in source_count_dict.items():
            take_count = request_count if redis_count > request_count else redis_count
            lrange_end = take_count -1
            take_value = r.lrange(source, 0, lrange_end)
            for value in take_value:
                self.response.append(eval(value))
            r.ltrim(source, take_count, redis_count)
        return self.response


class FeedBack(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        # print(self.request)
        task_info = self.get_argument('q', '[]').strip()
        yield self.async_get(task_info)

    @tornado.concurrent.run_on_executor
    def async_get(self, task_info):
        task_info = eval(parse.unquote(task_info))
        logger.info('slave端爬虫传递来{}个反馈！'.format(len(task_info)))
        if task_info:
            # tornado.ioloop.IOLoop.instance().add_timeout(0.1, callback=partial(insert_spider_result, task_info))
            # tornado.ioloop.IOLoop.instance().add_timeout(0.1, callback=partial(feed_back_date_task, task_info))
            insert_spider_result(task_info)
            feed_back_date_task(task_info)
        logger.info('tornado完成此次状态反馈和数据统计！')


application = tornado.web.Application([
    (r'/workload', GetTask),
    (r'/complete_workload', FeedBack),
    # (r'/admin', control.AdminHandler)
])

# os.popen('redis-cli shutdown')
# os.popen('redis-server &')
# os.popen('requirepass 123')
# os.popen('redis-cli shutdown')
# os.popen('redis-server &')
r = redis.Redis(host='10.10.180.145', port=6379,  decode_responses=True, db=10)


# def publish_hotel():
#     final_distribute_result = final_distribute(TaskType.Hotel)
#     print(final_distribute_result)
#     for source in task_source.hotel_source:
#         try:
#             len_source = r.llen(source)
#         except Exception as e:
#             logger.error("发生异常", exc_info=1)
#             len_source = 2000
#         if len_source < 1000:
#             logger.info('{}数量{}少于1000，开始进行补给。'.format(source, len_source))
#             for collection_name, mongo_tuple_list in final_distribute_result.items():
#                 for line in insert_mongo_data(source, collection_name, mongo_tuple_list):
#                     # try:
#                     #     print('line: %s' % (str(line)))
#                     # except Exception as e:
#                     #     pass
#                     content = line['task_args']['content']  # 注意往返飞机要拼接上日期，酒店不用
#                     source = line['source']
#                     content_list = content.split('&')
#                     content_list.insert(2, source)
#                     workload_key = '_'.join(content_list)
#                     data = {"content": content, "error": -1, "id": line['tid'], "is_assigned": 0, "priority": 0,
#                             "proxy": "10.10.114.35", "score": "-100", "source": source, "success_times": 0,
#                             "timeslot": 208, "update_times": 0, "workload_key": workload_key,
#                             "used_times": line['used_times'], "take_times": line['take_times'],
#                             "suggest": line['task_args']['suggest'],
#                             "suggest_type": line['task_args']['suggest_type'], "city_id": line['task_args']['city_id'],
#                             "tid": line['tid'], "collection_name": line['collection_name'], "feedback_times":line['feedback_times']}
#                     r.rpush(source, data)
#                     #暂时不开启
#                     update_running(collection_name, line['tid'], 1)
#
#     print('')


def publish_content(task_type):
    final_distribute_result = final_distribute(task_type)
    print(final_distribute_result)
    if task_type == TaskType.RoundFlight:
        source_list = task_source.round_flight_source
    elif task_type == TaskType.Flight:
        source_list = task_source.flight_source
    elif task_type == TaskType.Hotel:
        source_list = task_source.flight_source
    for source in source_list:
        try:
            len_source = r.llen(source)
        except Exception as e:
            logger.error("发生异常", exc_info=1)
            len_source = 2000
        if len_source < 1000:
            logger.info('{}数量{}少于1000，开始进行补给。'.format(source, len_source))
            for collection_name, mongo_tuple_list in final_distribute_result.items():
                for line in insert_mongo_data(source, collection_name, mongo_tuple_list):
                    if task_type == TaskType.RoundFlight:
                        content = line['task_args']['content'] + line['date']  # 注意往返飞机要拼接上日期，酒店不用
                    else:
                        content = line['task_args']['content']
                    source = line['source']
                    content_list = content.split('&')
                    content_list.insert(2, source)
                    workload_key = '_'.join(content_list)
                    if task_type == TaskType.Hotel:
                        data = {"content": content, "error": -1, "id": line['tid'], "is_assigned": 0, "priority": 0,
                            "proxy": "10.10.114.35", "score": "-100", "source": source, "success_times": 0,
                            "timeslot": 208, "update_times": 0, "workload_key": workload_key,
                            "used_times": line['used_times'], "take_times": line['take_times'],
                            "suggest": line['task_args']['suggest'],
                            "suggest_type": line['task_args']['suggest_type'], "city_id": line['task_args']['city_id'],
                            "tid": line['tid'], "collection_name": line['collection_name'], "feedback_times":line['feedback_times']}

                    else:
                        data = {"content": content, "error": -1, "id": line['tid'], "is_assigned": 0, "priority": 0,
                                "proxy": "10.10.114.35", "score": "-100", "source": source, "success_times": 0,
                                "timeslot": 208, "update_times": 0, "workload_key": workload_key,
                                "used_times": line['used_times'], "take_times": line['take_times'],
                                "tid": line['tid'], "collection_name": line['collection_name'],
                                "feedback_times": line['feedback_times']}
                    r.rpush(source, data)
                    # 暂时不开启
                    update_running(collection_name, line['tid'], 1)

    print('')


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(12345, '0.0.0.0')
    http_server.start()
    # tornado.ioloop.PeriodicCallback(publish_hotel, 100000).start()
    tornado.ioloop.PeriodicCallback(partial(publish_content, TaskType.Hotel), 100000).start()
    tornado.ioloop.PeriodicCallback(partial(publish_content, TaskType.Flight), 150000).start()
    tornado.ioloop.PeriodicCallback(partial(publish_content, TaskType.RoundFlight), 200000).start()
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()
