#!/usr/bin/python
# coding=utf-8

from com.mioji.common import task_class, crawl_type
from com.mioji.common.logger import logger
import static


class TaskQueue(object):

    def __init__(self, c_type, pkg_list):
        self.c_type = c_type
        # 优先级排列
        self.pkg_list = sorted(pkg_list, key=lambda p: str(p.level) + str(p.id))
        self.max_period_day = max([p.period_day for p in self.pkg_list])

        # 若当天任务分配完成，将分配后一天任务切片
        self.roll_offset = 0
        self.pkg_offset = 0

        # 任务生成断点子程序
        self.iter_func = self.__assign_iter()

    def get_offset(self):
        c_tp = self.pkg_list[self.pkg_offset]
        return [self.roll_offset, self.pkg_offset, c_tp.tp_offset, c_tp.task_offset]

    def re_init_today(self):
        for pkg in self.pkg_list:
            pkg.re_init_today()

        self.roll_offset = 0
        self.pkg_offset = 0

        self.iter_func = self.__assign_iter()

    def set_init_offset(self, args):
        roll_offset, pkg_offset, tp_offset, task_offset = args
        self.roll_offset = roll_offset
        self.pkg_offset = pkg_offset
        self.pkg_list[self.pkg_offset].set_offset(tp_offset, task_offset)

        self.iter_func = self.__assign_iter()

    def get(self, size):
        tmp = []
        try:
            for _ in xrange(size):
                task = self.iter_func.next().task()
                static.assign(task)
                tmp.append(task)
        except StopIteration, e:
            logger.warn('Queue {0} 没有可分配的任务了'.format(self.c_type))

        return tmp

    def __assign_iter(self):
        for roll_index in xrange(self.roll_offset, self.max_period_day):
            self.roll_offset = roll_index
            logger.info('Queue {type} start roll index: {0} pkg_offset: {1}'.format(roll_index, self.pkg_offset, type=self.c_type))
            for p in self.pkg_list[self.pkg_offset:]:
                if roll_index < p.period_day:
                    logger.info('Queue {type} roll index: {0} start pkg: {1} '
                                'tp_offset: {2} task_offset{3}'.format(roll_index, p.name, p.tp_offset, p.task_offset, type=self.c_type))
                    for task in p.assign_iter(roll_index):
                        yield task
                else:
                    # 该包内没有可分配任务
                    pass

                self.pkg_offset += 1

            # TODO 发邮件通知
            logger.info('roll:{0} assign complete'.format(roll_index))

            self.pkg_offset = 0

    def all_iter(self):
        for p in self.pkg_list:
            for task in p.all_iter():
                yield task

    def status(self):
        return 'ss'


def create_queue(pkg_ids):
    ps = [task_class.get_package(i) for i in pkg_ids]
    q = TaskQueue(crawl_type.T_HOTEL, ps)
    return q

if __name__ == '__main__':

    ps = [task_class.get_package(i) for i in xrange(1, 4)]
    q = TaskQueue(crawl_type.T_HOTEL, ps)
    # q.set_init_offset(0, 1, 2, 37)

    length = 0
    import time
    start = time.time()
    for _ in xrange(5000):
        ts = q.get(300)
        # print ts
        # for t in ts:
        #     print t.task()
        length += len(ts)

        print 'assigned count: {0}'.format(length)
        if length == 0:
            break

    print 'assigned offset ', q.get_offset()

    print 'time', time.time() - start



