import requests, bson, random
import time
import hashlib
import logging
from logger import Logger
from bson.objectid import ObjectId
from rabbitmq import pika_send
from conf import config

# coll = pika_send.client['case_result']['spider_result']
# coll.create_index([('qid', 1)], unique=True)

logger = Logger().get_logger()

host = 'localhost'

for i in range(1000):
    url = 'http://{host}:12345/workload'.format(host=host)
    try:
        res = requests.get(url, params={'data_type': "RoundFlight", 'count': config.per_retrieve_count}, timeout=30).text
        res = eval(res)
        result = []
        for t in res:
            random_content = str(time.time()) + str(t)
            qid = hashlib.md5(random_content.encode()).hexdigest()
            t['error'] = 0 if random.random() > 0.5 else 22
            t['qid'] = qid
            result.append(t)
        logger.info('result:%s'%result)
        logger.info('从master端获取到result')
        requests.post('http://{host}:12345/complete_workload'.format(host=host), data={'q': str(result)})
        print(i+1)
        logger.info('反馈结果给master')
    except Exception as e:
        logger.info(e)


#源名称，package_id, frequency(几天发一次),任务总数，取走的任务量，有反馈的任务数量，成功的任务数量，失败的任务数量，