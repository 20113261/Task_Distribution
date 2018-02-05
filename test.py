import unittest
import requests, json, random
import time
from bson.objectid import ObjectId
from bson import json_util

host = 'localhost'

class TestCase(unittest.TestCase):
    def test_gettask(self):

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

            requests.post('http://{host}:12345/complete_workload'.format(host=host), data={'q': str(result)})
            # time.sleep(5)


if __name__ == '__main__':
    unittest.main()
