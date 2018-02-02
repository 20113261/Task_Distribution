import gevent
import random
import pika_send
import datetime
import math
from rabbitmq.consumer import connect_rabbitmq


task_level =\
    {'flight': [0,1,2,3,4],
    'Round': [12, 13, 14],
    'multiflight_task_level': [0]}

product_connection = connect_rabbitmq()
channel = product_connection.channel()
channel.queue_declare(queue='')

total_task_level_dict = {}
collection_task_level = {}
level_dict = {}
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
now_time = datetime.datetime.now()
next_day_str = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'
next_day = datetime.datetime.strptime(next_day_str,"%Y-%m-%d %H:%M:%S")
print(next_day)

# print(datetime.datetime.strptime("2018-02-03 00:00:00","%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S'))
shares = (next_day - datetime.datetime.now()).total_seconds()/60/5
time_shares = math.ceil(shares)
per_share_count = math.ceil(total_count/time_shares)
print(time_shares, shares, per_share_count)

per_level = sorted(per_level_dict.items(), key=lambda d: d[0])

distribute_result = {}
def distribute(per_share_count, per_level):
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
            distribute(per_share_count - i[1], per_level[1:])
    print(per_level[1:])
distribute(4000, per_level)
print(distribute_result)

final_level_count = {}
# for key, value in distribute_result.items():
#     mongo_count = level_dict.get(key, {})
#     for coll