#!/usr/bin/python
# coding=utf-8

from com.mioji.common import today, crawl_type, base_value, task_class
from com.mioji.common.logger import logger
import load_task, task_queue, retry_handle, static
from com.mioji.dao import file_dao


class App(object):

    def __init__(self):
        load_task.load_task()

        # todo init others crawl queue
        self.queue = {
            crawl_type.T_HOTEL: task_queue.create_queue([1, 2, 3])
        }
        self.re_init_today()
        self.retry_handler = retry_handle.FeedbackHandle()

        self.__init_stat_from_cache()

    def safe_re_init_today(self):
        pass

    def re_init_today(self):
        static.init_static()
        today.today = today.Today()
        self.retry_handler = retry_handle.FeedbackHandle()
        for _, queue in self.queue.iteritems():
            queue.re_init_today()

    def get(self, types, count):
        if not types:
            return []

        type_length = len(types)
        t_count = count/type_length

        # todo ListHotel ... 转换 crawl_type
        types = ['hotel']

        ts = []
        for c_type in types:
            ts += self.retry_handler.get(c_type, t_count)
            retry_count = len(ts)
            need_more_count = t_count - retry_count
            if need_more_count <= 0:
                logger.info('get {0} task from retry: {1}'.format(c_type, retry_count))
                continue
            q = self.queue.get(c_type)
            if q is not None:
                ts += q.get(need_more_count)
            logger.info('get {0} task assigned {1} from retry: {2}'
                        .format(c_type, len(ts) - retry_count, retry_count))
        return ts

    def feedback(self, task_list):
        self.retry_handler.feed_back(task_list)

    def user_add(self, task_list):
        # todo
        pass

    def _stat(self):
        # todo
        stat = {}
        for k, q in self.queue.iteritems():
            stat[k] = q.get_offset()
        return stat

    def status(self):
        return {
            'stat': self._stat(),
            'static': static.package_static,
            'source_static': static.source_static
        }

    def stop(self):
        # todo store stat
        self.__cache()
        import sys
        sys.exit()

    def __init_stat_from_cache(self):
        today_tag = today.today.today_tag
        stat = file_dao.load_dict('app_stat_{0}.stat'.format(today_tag))
        if stat:
            for k,v in stat.iteritems():
                self.queue[k].set_init_offset(v)

        retry_query = file_dao.load_dict('retry_queue_{0}.stat'.format(today_tag))
        if retry_query:
            self.retry_handler.retry_queue = retry_query

        retry_count = file_dao.load_dict('retry_count_{0}.stat'.format(today_tag))
        if retry_count:
            self.retry_handler.retry_count_cache = retry_count

        static_cache = file_dao.load_dict('static_{0}.stat'.format(today_tag))
        if static_cache:
            static.init_by_cache(static_cache)

    def __cache(self):
        today_tag = today.today.today_tag
        file_dao.store_dict('app_stat_{0}.stat'.format(today_tag), self._stat())
        file_dao.store_dict('retry_queue_{0}.stat'.format(today_tag), self.retry_handler.retry_queue)
        file_dao.store_dict('retry_count_{0}.stat'.format(today_tag), self.retry_handler.retry_count_cache)
        file_dao.store_dict('static_{0}.stat'.format(today_tag), static.stat())

app = App()


if __name__ == '__main__':

    import random, json, time

    start = time.time()
    print app.status()
    for x in range(2):
        ts = app.get(['hotel'], 100)
        feed_ts = json.loads(json.dumps(ts))
        for t in feed_ts:
            t['error'] = 0 if random.random() > 0.5 else 22
        feed_ts = json.loads(json.dumps(feed_ts))
        app.feedback(feed_ts)

    app.stop()
    print app.status()
    print 'time', time.time() - start