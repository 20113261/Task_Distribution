import requests, bson, random
import time
import hashlib
from bson.objectid import ObjectId
from rabbitmq import pika_send

# coll = pika_send.client['case_result']['spider_result']
# coll.create_index([('qid', 1)], unique=True)

host = 'localhost'

for i in range(1000):
    url = 'http://{host}:12345/workload'.format(host=host)
    res = requests.get(url, params={'data_type': "RoundFlight", 'count': 200}).text
    res = eval(res)
    result = []
    for t in res:
        random_content = str(time.time()) + str(t)
        qid = hashlib.md5(random_content.encode()).hexdigest()
        t['error'] = 0 if random.random() > 0.5 else 22
        t['qid'] = qid
        result.append(t)
    print(result)
    # requests.post('http://{host}:12345/complete_workload'.format(host=host), data={'q': str(result)})
    print(i+1)