#!/usr/bin/env python
import pika
import pymongo
import json
import time
from conf import task_source, config
from common import InsertDateTask
from . import pika_send

username = 'hourong'  # 指定远程rabbitmq的用户名密码
pwd = '1220'
user_pwd = pika.PlainCredentials(username, pwd)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd))
channel = connection.channel()


client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]
db = None
print(date_task_db.collection_names())



def gen_queue(sources):
    for source in sources:
        channel.queue_declare(queue=source)

def receive_mongo_data(message_count, collection_name):
    if message_count < 100:
        collection = date_task_db[collection_name]
        datas = collection.find({}).limit(10)
        queue_name = collection_name.split('_')[3]
        for line in datas:
            print(line)
            pika_send.channel.basic_publish(exchange='producer', routing_key=queue_name, body=str(line))


# source_list = []
# flight_source = task_source.flight_source
# round_flight_source = task_source.round_flight_source
# multi_flight_source = task_source.multi_flight_source
# source_list.extend(flight_source)
# source_list.extend(round_flight_source)
# source_list.extend(multi_flight_source)
#
# gen_queue(source_list)


# base_collections = date_task_db['']

# for i in range(100):
#     channel.basic_publish(exchange='',
#                       routing_key='hello',
#                       body='hello_world')


# channel = connection.channel()
# channel.queue_declare(queue='hello')
# for i in range(10):
#     channel.basic_publish(exchange='',
#                           routing_key='hello',
#                           body=str(i))
#     print(" [x] Sent 'Hello World!'")
# connection.close()

if __name__ == '__main__':
    receive_mongo_data(2, collection_name='DateTask_Round_Flight_cheapticketsRoundFlight_20180122')