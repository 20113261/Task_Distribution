#!/usr/bin/python
# coding=utf-8

from com.mioji.common import crawl_type, base_value
from com.mioji.common.task_class import Task
from com.mioji.common.logger import logger
import static


class FeedbackHandle(object):

    TAG_ERROR = 'error'
    TAG_CONTENT = 'content'

    def __init__(self):
        self.retry_queue = {}
        for c_type in crawl_type.CRAWL_TYPES:
            self.retry_queue[c_type] = []

        self.retry_count_cache = {}

    def get(self, c_type, size):
        q = self.retry_queue[c_type]
        ts = q[:size]
        del q[:size]
        for t in ts:
            self._retry_count_add(t)

        static.assign_tasklist(ts, isretry=True)
        logger.info('retry queue {0} size: {1}'.format(c_type, len(q)))
        return ts

    def feed_back(self, args):
        task_param_error = []
        wait_to_retry = []
        for task in args:
            static.feedback(task)
            code = task[FeedbackHandle.TAG_ERROR]
            if code == 0:
                # 成功后删除缓存次数记录
                # 重试1次的话直接,任务被pop出去，不会二次生成
                self._remove_retry_cache(task)
                continue
            elif code == 12:
                # 12 任务格式错误应检查并处理
                task_param_error.append(task)
            else:
                wait_to_retry.append(task)

        if task_param_error:
            logger.warn('task error task {0}'.format(task_param_error))

        self.__error_task_can_retry_assign(wait_to_retry)

    def __error_task_can_retry_assign(self, task_list):
        for task in task_list:
            # 重试一次
            if self._get_task_retry_count(task) >= 1:
                # 当天不会在分配了retry count 没有用了
                self._remove_retry_cache(task)
                continue
            assign_task = _a_task(task)
            c_type = base_value.find_crawl_type_by_section_name(assign_task[Task.TAG_SOURCE])
            self.retry_queue[c_type].append(assign_task)

    def _retry_count_add(self, task):
        key = _retry_task_key(task)
        self.retry_count_cache.setdefault(key, 0)
        self.retry_count_cache[key] += 1

    def _get_task_retry_count(self, task):
        key = _retry_task_key(task)
        return self.retry_count_cache.get(key, 0)

    def _remove_retry_cache(self, task):
        key = _retry_task_key(task)
        if self.retry_count_cache.has_key(key):
            del self.retry_count_cache[key]


def _retry_task_key(task):
    key = '{0}'.format(task[Task.TAG_ID])
    return key


def _a_task(task):
    return {
        Task.TAG_CRYPE: task[Task.TAG_CRYPE],
        Task.TAG_CONTENT: task[Task.TAG_CONTENT],
        Task.TAG_SOURCE: task[Task.TAG_SOURCE],
        Task.TAG_ID: task[Task.TAG_ID]
    }


if __name__ == '__main__':
    list = [{1:2},{1:3}]

    print list[0:3]
    del list[0:3]
    print list

