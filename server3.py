#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/02
# @Author  : zhangxiaopeng
# @Site    :
# @File    : server.py
# @Software: PyCharm
# !/usr/bin/python
# coding=utf-8
import threading
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.concurrent
import json
import time
import datetime
import math
import pika
import traceback
from logger_file import get_logger
from bson import json_util
from bson.objectid import ObjectId
from rabbitmq import pika_send
from tornado.options import define
from conf import config
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rabbitmq.consumer import connect_rabbitmq, insert_spider_result, feed_back_date_task, slave_take_times
from tornado.locks import Condition
from pika.adapters.blocking_connection import BlockingConnection
from pika.adapters.tornado_connection import TornadoConnection
from tornado.locks import Condition

logger = get_logger('server3')
port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()

class GetTask(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(20)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        print(self.request)
        print("Hello World")
        self.data_type = self.get_argument('data_type', '').strip()
        # self.data_type = date_type.split('_')
        self.request_count = math.ceil(float(self.get_argument('count', '0').strip())/6)
        yield [self.run()]
        self.condition = Condition()

        yield self.condition.wait()
        print('*' * 20)
        if not self._finished:
            self.write(str(self.response))
            self.finish()
            slave_take_times(self.response)
        print('finish this request！**********************')


    @tornado.concurrent.run_on_executor
    def run(self):
        if self.data_type:
            print(self.data_type)
            self.response = []
            self.available_queue_count = 0
            self.nack_count = 0

            self.connect()

        else:
            self.write('[]')
            self.finish()

    def connect(self):
        try:
            username = 'hourong'  # 指定远程rabbitmq的用户名密码
            pwd = '1220'
            user_pwd = pika.PlainCredentials(username, pwd)
            param = pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd)

            self.connection = TornadoConnection(param, on_open_callback=self.on_connected)

        except Exception as e:
            logger.error('Something went wrong... %s', e)

    def on_connected(self, connection):
        logger.info('Succesfully connected to rabbitmq')
        for collection_name in pika_send.date_task_db.collection_names():
            if self.data_type in collection_name:
                queue_name = collection_name.split('_')[-2]
                self.connection.channel(partial(self.on_channel_open, collection_name=collection_name,queue_name=queue_name))

    def on_channel_open(self, new_channel, **kwargs):
        """When the channel is open this is called"""
        logger.info('Opening channel to rabbitmq')

        # global channel
        channel = new_channel
        channel.queue_declare(queue=kwargs['queue_name'],
                              callback=partial(self.callback_first,
                              collection_name=kwargs['collection_name'],
                              consumer_channel=channel)
                              )

    def callback_second(self, ch, method, properties, body, **kwargs):
        if method.delivery_tag <= kwargs['ack_count']:
            try:
                ch.basic_ack(method.delivery_tag, multiple=True)
                body = eval(str(body, 'utf-8'))
                body['collection_name'] = kwargs['collection_name']
                print(" [x] Received %r" % body)
                self.response.append(body)
                if method.delivery_tag == kwargs['ack_count']:
                    ch.close()
                    self.nack_count += 1
                    if self.nack_count == self.available_queue_count:

                        self.condition.notify()
            except Exception as e:
                traceback.print_exc()
                print('Exception', e, '请求:', self.request)
        # else:
        #     ch.basic_nack(method.delivery_tag, multiple=True)
        #     ch.close()
        #     self.nack_count += 1
        #     if self.nack_count == self.available_queue_count:
        #         print('*'*20)
        #         self.write(str(self.response))
        #         self.finish()
        #         self.condition.notify()

    def callback_first(self, frame, **kwargs):
        message_count = frame.method.message_count
        channel = kwargs['consumer_channel']
        if message_count:
            # retrieve_count = config.per_retrieve_count if message_count >= config.per_retrieve_count else message_count
            retrieve_count = self.request_count if message_count >= self.request_count else message_count
            logger.info('将从%s中取出%d个消息' % (frame.method.queue, retrieve_count))
            self.available_queue_count += 1
            print(kwargs['collection_name'].split('_')[3], message_count)
            channel.basic_qos(prefetch_count=retrieve_count)
            channel.basic_consume(consumer_callback=partial(self.callback_second, ack_count=retrieve_count,
                                                            collection_name=kwargs['collection_name']),
                                  queue=frame.method.queue, no_ack=False)
        else:
            channel.close()


class FeedBack(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(10)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        print(self.request)
        logger.info('slave端爬虫传递来反馈!')
        task_info = self.get_argument('q', '[]').strip()
        yield self.async_get(task_info)

    @tornado.concurrent.run_on_executor
    def async_get(self, task_info):
        task_info = eval(task_info)
        # logger.info(task_info)
        if task_info:
            self.finish()
            insert_spider_result(task_info)
            feed_back_date_task(task_info)
        logger.info('tornado完成此次状态反馈和数据统计！')


# task_list = json.loads(task_info)
#         yield self.async_get(task_list)
#
#     @tornado.web.asynchronous
#     @tornado.gen.coroutine
#     def post(self):
#         task_info = self.get_argument('q', '[]').strip()
#         task_list = json.loads(task_info)
#         self.write('')
#         yield self.async_get(task_list)




application = tornado.web.Application([
    (r'/workload', GetTask),
    (r'/complete_workload', FeedBack),
    # (r'/admin', control.AdminHandler)
])


if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(12345, '0.0.0.0')
    http_server.start()
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()
