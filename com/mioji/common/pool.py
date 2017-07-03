
from gevent.pool import Pool

default_pool = Pool(32)
pools_config = {'dao_pool': 20}
pools = {}
for k, size in pools_config.iteritems():
    pools[k] = Pool(size)


def get_pool(name):
    return pools.get(name, default_pool)