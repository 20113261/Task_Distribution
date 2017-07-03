#!/usr/bin/python
# coding=utf-8

import json, time
import base_value, crawl_type
from logger import logger
import utils, today

ONE_DAY_SEC = 24*60*60

TYPE_TASK_PARAM_DIC = \
    {crawl_type.T_HOTEL: {},
     crawl_type.T_FLIGHT: {}}

TYPE_TASK_DIC = \
    {crawl_type.T_HOTEL: {},
     crawl_type.T_FLIGHT: {}}


class Package(object):
    """
    args (1, '酒店rank0:[7,90]/天', '', 0, 24, crawl_type.T_HOTEL, '{"day": [7, 90], "occ": [2, 2]}')
    """

    def __init__(self, args):
        self.id = args[0]
        self.name = args[1]
        self.desc = args[2]
        self.level = args[3]
        self.period = args[4]
        self.period_day = int(self.period/24)
        self.crawl_type = args[5]
        self.iteration = json.loads(args[6])
        self.iteration_values = None
        self.iteration_values_length = 0
        self._task_param_list = []

        # task size
        self._task_size = 0
        self._source_task_size = {}

        self.assign_day_segs = None
        self.tp_offset = 0
        self.task_offset = 0

        self.__init_iterations()
        self.__init_today()

    def __init_iterations(self):
        """
        初始化迭代信息 如 酒店: 抓取天 size([7,90]) * 入住数 size([2,2]) * [入住天数 size([1,3])]
        :return: 
        """
        keys = sorted(self.iteration.keys())
        values = []
        for k in keys:
            v = self.iteration[k]
            # 区间表示
            values = utils.iteration_append(values, xrange(v[0], v[1] + 1))
        self.iteration_values = values
        self.iteration_values_length = len(self.iteration_values)

    def __init_today(self):
        """
        初始化当前天切片
        :return: 
        """
        day_off = int(time.time() / ONE_DAY_SEC) % self.period_day
        tmp_seg = []
        for d in xrange(self.period_day):
            tmp_seg.append((d + day_off) % self.period_day)
        self.assign_day_segs = tmp_seg

        # 初始化偏移量置零
        self.tp_offset = 0
        self.task_offset = 0

    def re_init_today(self):
        """
        重置当天信息。跨天时由外部调用
        :return: 
        """
        self.__init_today()

    def set_offset(self, tp_offset, task_offset):
        self.tp_offset = tp_offset
        self.task_offset = task_offset

    def assign_iter(self, segment_index):
        assign_day = self.assign_day_segs[segment_index]
        logger.info('assign_iter assign_day {0}'.format(assign_day))
        for tp in self._task_param_list[self.tp_offset:]:
            index = 0
            # logger.info('assigned package tp start {0}'.format(tp))
            for task in tp.all_iter():
                if index == assign_day:
                    yield task

                index += 1
                if index >= self.period_day:
                    index = 0
                self.task_offset += 1

            self.tp_offset += 1
            self.task_offset = 0

        logger.info('package assign_iter complete assign_day: {0}'.format(assign_day))
        self.tp_offset = 0

    def all_iter(self):
        for tp in self._task_param_list:
            for task in tp.all_iter():
                yield task

    def get_task_param_list(self):
        return self._task_param_list

    def add_task_param(self, task_param):
        self._task_param_list.append(task_param)
        self._task_size += task_param.task_length
        for s_id in task_param.source_ids:
            self._source_task_size.setdefault(s_id, 0)
            self._source_task_size[s_id] += self.iteration_values_length

    def task_param_length(self):
        return len(self._task_param_list)

    @property
    def task_length(self):
        return self._task_size

    @property
    def oneday_task_length(self):
        return self._task_size / self.period_day

    def source_task_length(self, source_id):
        return self._source_task_size.get(source_id, 0)

    def oneday_source_task_length(self, source_id):
        return self._source_task_size.get(source_id, 0) / self.period_day


class TaskParam(object):
    C_TYPE = crawl_type.T_HOTEL

    def __init__(self, id, source_info):
        self.id = id
        self.package_id = -1
        self.source_ids = json.loads(source_info)

    def content(self, iteration_info):
        raise Exception('u must reload this func and do not call super func')

    @property
    def task_length(self):
        return len(self.source_ids) * get_package(self.package_id).iteration_values_length

    def all_iter(self):
        pkg = get_package(self.package_id)
        for i in pkg.iteration_values:
            for source_id in self.source_ids:
                task = Task((self.id, source_id, i, TaskParam.C_TYPE))
                task.task_param_id = self.id
                task.package_id = self.package_id
                yield task


class HotelTaskParam(TaskParam):
    # 默认1晚
    FT_CONTENT = '{city}&{occ}&1&{date}'
    C_TYPE = crawl_type.T_HOTEL

    __slots__ = ('id', 'city_id', 'source_ids')

    def __init__(self, args):
        TaskParam.__init__(self, args[0], args[2])
        self.city_id = args[1]

    def content(self, iteration_info):
        day, occ = iteration_info
        date = today.today.day_offset_date_str(day)
        return HotelTaskParam.FT_CONTENT.format(city=self.city_id, date=date, occ=occ)

    def __str__(self):
        return '{0}-{1}'.format(HotelTaskParam.C_TYPE, self.city_id)


class Task(object):

    __slots__ = ('id', 'task_param_id', 'iteration_info', 'source_id', 'crawl_history_id', 'last_succes_time',
                 'crawl_times', 'success_times', 'updatetime', 'package_id', 'crawl_type')

    TAG_SOURCE = 'source'
    TAG_CONTENT = 'content'
    TAG_ID = 't_id'
    TAG_CRYPE = 'crawl_type'

    def __init__(self, args):
        self.task_param_id = args[0]
        self.source_id = args[1]
        self.iteration_info = args[2]
        self.crawl_type = args[3]

        self.crawl_history_id = -1
        self.crawl_times = 0
        self.success_times = 0
        self.last_succes_time = 0
        self.updatetime = 0
        self.package_id = -1

    @property
    def id(self):
        return json.dumps([crawl_type.T_HOTEL, self.task_param_id])

    def content(self):
        # find Taskparam
        task_param = get_task_param(self.crawl_type, self.task_param_id)
        return task_param.content(self.iteration_info.split(':'))

    def task(self):
        return {
            Task.TAG_CRYPE: self.crawl_type,
            Task.TAG_CONTENT: self.content(),
            Task.TAG_SOURCE: base_value.find_section_name(self.crawl_type, self.source_id),
            Task.TAG_ID: [self.package_id, self.task_param_id, self.source_id, self.iteration_info],
        }


def set_init_model(c_type, task_params, crawl_task, relation):
    pid_pkgid_dic = {}
    for r in relation:
        pid_pkgid_dic[r[1]] = r[0]

    for tp in task_params:
        tp.package_id = pid_pkgid_dic.get(tp.id, -1)
        TYPE_TASK_PARAM_DIC[c_type][tp.id] = tp
        pkg = get_package(tp.package_id)
        if pkg is not None:
            pkg.add_task_param(tp)

    del pid_pkgid_dic

    for ct in crawl_task:
        TYPE_TASK_DIC[c_type][ct.id] = ct
        tp = get_task_param(c_type, ct.task_param_id)
        if tp is not None:
            ct.package_id = tp.package_id
            tp.task_list.append(ct)


def set_init_task_model(c_type, task_params, relation):
    pid_pkgid_dic = {}
    for r in relation:
        pid_pkgid_dic[r[1]] = r[0]

    for tp in task_params:
        tp.package_id = pid_pkgid_dic.get(tp.id, -1)
        TYPE_TASK_PARAM_DIC[c_type][tp.id] = tp
        pkg = get_package(tp.package_id)
        # tp.init_task()
        if pkg is not None:
            pkg.add_task_param(tp)

    del pid_pkgid_dic


def get_task_param(c_type, id):
    return TYPE_TASK_PARAM_DIC.get(c_type, {}).get(id)


def get_task_(c_type, id):
    return TYPE_TASK_DIC.get(c_type, {}).get(id)


def get_package(id, exist={}):
    p = exist.get(id, None)
    if p is None:
        p = Package(base_value.package_list[id-1])
        exist[id] = p
    return p


if __name__ == '__main__':

    p = get_package(1)
    p = get_package(1)
    for x in p.iteration_list():
        print x