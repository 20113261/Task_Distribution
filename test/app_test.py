import unittest


class MyTestCase(unittest.TestCase):

    def test_something(self):
        import random, json
        from com.mioji.model.app import app
        print app.status()
        for x in range(50000):
            ts = app.get(['hotel'], 600)
            feed_ts = json.loads(json.dumps(ts))
            for t in feed_ts:
                t['error'] = 0 if random.random() > 0.5 else 22
            feed_ts = json.loads(json.dumps(feed_ts))
            app.feedback(feed_ts)

        app.stop()
        print app.status()
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
