#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:01
# @Author  : Hou Rong
# @Site    :
# @File    : server.py
# @Software: PyCharm
# !/usr/bin/python
# coding=utf-8
import tornado.web
import tornado.ioloop
import tornado.httpserver
import tornado.concurrent
import json
from rabbitmq import pika_send
from tornado.options import define
from conf import config
from concurrent.futures import ThreadPoolExecutor
from rabbitmq.mongo_data import receive_mongo_data
from functools import partial
from rabbitmq.consumer import ExampleConsumer

port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()


# class GetTask(tornado.web.RequestHandler):
#     executor = ThreadPoolExecutor(10)
#     @tornado.concurrent.run_on_executor
#     def callback(self, ch, method, properties, body):
#         pika_send.channel.basic_ack(method.delivery_tag)
#         print(" [x] Received %r" % body)
#
#         self.response.append(body)
#         # if method.delivery_tag
#
#     @tornado.concurrent.run_on_executor
#     def call(self, frame, **kwargs):
#         message_count = frame.method.message_count
#         receive_mongo_data(message_count, kwargs['collection_name'])
#         print(kwargs['collection_name'].split('_')[3], message_count)
#
#     @tornado.web.asynchronous
#     @tornado.gen.coroutine
#     def get(self):
#         print("Hello World")
#         date_type = self.get_argument('data_type', '').strip()
#         count = int(self.get_argument('count', '0').strip())
#
#         yield self.async_get(date_type.split('_'), count)
#
#     @tornado.concurrent.run_on_executor
#     def async_get(self, types, count):
#         # consumer = ExampleConsumer()
#         if types:
#             self.count = 0
#             print(types)
#             # task_list = app.get(types, count)
#             # self.write(json.dumps(task_list))
#             self.response = []
#             for type in types:
#                 for collection_name in pika_send.date_task_db.collection_names():
#                     queue_name = collection_name.split('_')[3]
#                     if type in collection_name:
#                         print('ok!')
#                         self.collection_name = collection_name
#                         pika_send.channel.queue_declare(
#                             queue=queue_name,
#                             callback=partial(self.call, collection_name=collection_name)
#                         )
#
#                         # print(self.message_count)
#                         # receive_mongo_data()
#                         # pika_send.channel.basic_publish(exchange='direct', routine_key='db', body='')
#
#                         pika_send.channel.basic_qos(prefetch_count=1)
#                         pika_send.channel.basic_consume(self.callback, queue=queue_name, no_ack=False)
#                         self.count += 1
#                     else:
#                         print('no')
#             self.write(str(self.response))
#             self.finish()
#         else:
#             self.write('[]')

@tornado.concurrent.run_on_executor
def a():
    for collection_name in pika_send.date_task_db.collection_names():
        queue_name = collection_name.split('_')[3]
        pika_send.channel.queue_declare(
            queue=queue_name,
            callback=partial(call, collection_name=collection_name)
        )

# @tornado.concurrent.run_on_executor
# def call(frame, **kwargs):
#     message_count = frame.method.message_count
#     receive_mongo_data(message_count, kwargs['collection_name'])
#     print(kwargs['collection_name'].split('_')[3], message_count)

application = tornado.web.Application([

])

if __name__ == '__main__':
    application.pika = pika_send.PikaClient()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(12345, '0.0.0.0')
    http_server.start()
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.add_timeout(500, application.pika.connect)
    ioloop.run_sync(a)
