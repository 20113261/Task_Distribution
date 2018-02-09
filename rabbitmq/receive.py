#!/usr/bin/env python
import pika
import pymongo
from conf import config

username = 'hourong'   #指定远程rabbitmq的用户名密码
pwd = '1220'
user_pwd = pika.PlainCredentials(username, pwd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd))

channel = connection.channel()

# channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]

# for db in date_task_db.collection_names():
#     queue_name = db.split('_')[3]
#     channel.basic_consume(callback,
#                           queue='pricelineRoundFlight',
#                           no_ack=True)

def consume():
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(callback,
                          queue='hello',
                          no_ack=False)
    channel.start_consuming()

for i in range(5):
    channel.basic_publish('consumer', 'hello', 'hello_world!')
