import unittest
import requests, json, random

host = 'localhost'


class MyTestCase(unittest.TestCase):

    def test_get_task(self):
        for i in xrange(10000):
            url = 'http://{host}:8087/workload'.format(host=host)
            res = requests.get(url, params={'data_type': "hotel", 'count': 6000}).text
            ts = json.loads(res)
            for t in ts:
                t['error'] = 0 if random.random() > 0.5 else 22

            requests.post('http://{host}:8087/complete_workload'.format(host=host), data={'q': json.dumps(ts)})


if __name__ == '__main__':
    unittest.main()
