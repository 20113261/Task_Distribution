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
import time
import pika
from bson import json_util
from bson.objectid import ObjectId
from rabbitmq import pika_send
from tornado.options import define
from conf import config
from concurrent.futures import ThreadPoolExecutor
# from rabbitmq.mongo_data import receive_mongo_data
from functools import partial
from rabbitmq.consumer import connect_rabbitmq, insert_spider_result, feed_back_date_task
from tornado.locks import Condition
from pika.adapters.blocking_connection import BlockingConnection

port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()


class GetTask(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(10)

    def callback(self, ch, method, properties, body, **kwargs):
        left_count = ch.get_waiting_message_count()
        if method.delivery_tag == kwargs['ack_count']:
            try:
                ch.basic_ack(method.delivery_tag, multiple=True)
                # body = eval(str(body, 'utf-8'))
                # body['collection_name'] = kwargs['collection_name']
                print(" [x] Received %r" % body)

                self.response.append(body)
                ch.stop_consuming()
            except Exception as e:
                print('Exception', e)
        elif method.delivery_tag >= kwargs['ack_count']:
            ch.basic_nack(method.delivery_tag, multiple=True)
            ch.stop_consuming()

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        global consumer_connection
        print("Hello World")
        if consumer_connection.is_closed:
            consumer_connection = connect_rabbitmq(consumer_connection)

        date_type = self.get_argument('data_type', '').strip()
        count = int(self.get_argument('count', '0').strip())/5

        yield [self.async_get(date_type.split('_'), int(count))]

    @tornado.concurrent.run_on_executor
    def async_get(self, types, count):
        if types:
            print(types)
            self.response = []
            for type in types:
                for collection_name in pika_send.date_task_db.collection_names():
                    channel = consumer_connection.channel()
                    channel.basic_qos(prefetch_count=int(count))
                    if collection_name == 'DateTask_Round_Flight_pricelineRoundFlight_20180205':
                        continue
                    queue_name = collection_name.split('_')[3]
                    # if collection_name in ['DateTask_Round_Flight_cheapticketsRoundFlight_20180205', 'DateTask_Round_Flight_orbitzRoundFlight_20180205']:
                    #     continue
                    if type in collection_name:
                        try:
                            print('ok!')
                            self.collection_name = collection_name
                            channel.get_waiting_message_count()
                            channel.basic_consume(consumer_callback=partial(self.callback, ack_count=count,
                                                                            collection_name=collection_name),
                                                  queue=queue_name, no_ack=False)

                            channel.start_consuming()
                        except pika.exceptions.RecursionError as e:
                            print(e)
                        except pika.exceptions.InvalidFrameError as e:
                            print(e)
                        except Exception as e:
                            print(e)
                            # consumer_connection.close()
                            # consumer_connection = connect_rabbitmq(consumer_connection)
                        # pika_send.channel.queue_declare(
                        #     queue=queue_name,
                        #     callback=partial(self.call, collection_name=collection_name)
                        # )
                        # print(self.message_count)
                        # receive_mongo_data()
                        # pika_send.channel.basic_publish(exchange='direct', routine_key='db', body='')

                        # pika_send.channel.basic_consume(self.callback, queue='hello', no_ack=False)

                    else:
                        print('no')

            print('---', self.response)
            self.write(str(self.response))
            channel.close()
            self.finish()
        else:
            self.write('[]')
            channel.close()
            self.finish()

class FeedBack(tornado.web.RequestHandler):
    # executor = ser_excutor.executor_pool

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        print('FeedBack!')
        task_info = self.get_argument('q', '[]').strip()
        task_info = eval(task_info)
        insert_spider_result(task_info)
        feed_back_date_task(task_info)
        print(task_info)


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
#
#     @tornado.concurrent.run_on_executor
#     def async_get(self, task_list):
#         if task_list:
#             app.feedback(task_list)


application = tornado.web.Application([
    (r'/workload', GetTask),
    (r'/complete_workload', FeedBack),
    # (r'/admin', control.AdminHandler)
])


if __name__ == '__main__':
    # application.pika = BlockingConnection(pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd))
    consumer_connection = None
    consumer_connection = connect_rabbitmq(consumer_connection)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(12345, '0.0.0.0')
    http_server.start()
    ioloop = tornado.ioloop.IOLoop.instance()
    # ioloop.add_timeout(500, BlockingConnection(pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd)))
    ioloop.start()

