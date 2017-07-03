
import redis
import json
# cls = redis.ConnectionPool.from_url('redis://[:MiojiPikaOrz]@10.10.122.15:9226/0')
# pool = redis.ConnectionPool(cls)
# r = redis.Redis(connection_pool=pool)
r = redis.Redis(host='10.10.122.15', port=9226, password='MiojiPikaOrz', db=0)
print r.get('["hotel", 5558, "85:2", 6]')

def insert_crawl_task(c_type, args):
    #(task_param_id, iteration_info, source_id)
    print '$' * 10
    pip = r.pipeline(transaction=False)
    for arg in args:
        key = json.dumps((c_type,) + arg)
        # r.set(key, '{}')
        print pip.set(key, '{}')
        print key
    pip.execute()
