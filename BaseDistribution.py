#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:08
# @Author  : Hou Rong
# @Site    : 
# @File    : BaseDistribution.py
# @Software: PyCharm
import redis
import json
import datetime
from Task import Task
from logger_file import get_logger
from collections import defaultdict

logger = get_logger("BaseDistribution")

MIN_FLIGHT_TASK_SIZE = 50000
MIN_TRAIN_TASK_SIZE = 10000
MIN_BUS_TASK_SIZE = 10000
MIN_HOTEL_TASK_SIZE = 50000
MIN_ROUNDFLIGHT_TASK_SIZE = 20000
MIN_MULTIFLIGHT_TASK_SIZE = 20000
ADJUST_FREQUENCY_RATE = 0.4


class BaseDistribution(object):
    def __init__(self):
        self.source_rate_map = {}
        self.m_feedback_source_vec = []

        # queue
        self.flight_task_queue = []
        self.train_task_queue = []
        self.bus_task_queue = []
        self.hotel_task_queue = []
        self.roundflight_task_queue = []
        self.multiflight_task_queue = []

        # 记录当次需重试的任务，重试后清空map
        self.flight_retry_task_map = {}
        self.train_retry_task_map = {}
        self.bus_retry_task_map = {}
        self.hotel_retry_task_map = {}
        self.roundflight_retry_task_map = {}
        self.multiflight_retry_task_map = {}

        # 记录任务重试的次数
        self.flight_retry_times_map = {}
        self.hotel_retry_times_map = {}
        self.train_retry_times_map = {}
        self.bus_retry_times_map = {}
        self.roundflight_retry_times_map = {}
        self.multiflight_retry_times_map = {}

        # report
        self.source_statistic_info = defaultdict(int)
        self.type_statistic_info = defaultdict(int)
        self.source_error_code_info = defaultdict(int)
        self.feedback_info = defaultdict(int)

        today = datetime.datetime.today()
        self.m_cur_date = today.strftime("%Y%m%d")
        self.m_yes_date = (today - datetime.timedelta(days=1)).strftime("%Y%m%d")
        logger.info("[cur: {}][yes: {}]".format(self.m_cur_date, self.m_yes_date))

        self.r = redis.Redis()

        error_key_vec = []
        error_key_map = defaultdict(str)
        error_key_map.update({k: self.r.get(k) for k in self.m_feedback_source_vec})

        source_frequency_map = {}
        source_summary_info = {}
        for k, v in source_frequency_map.items():
            for i in range(288):
                frequency_key = "{}_frequency_{}_{}".format(self.m_cur_date, k, i)

                if not self.r.exists(frequency_key):
                    self.r.set(frequency_key, v)
            error_key_vec.append("{}_error_{}_0".format(self.m_yes_date, k))
            error_key_vec.append("{}_error_{}_1".format(self.m_yes_date, k))
            self.m_feedback_source_vec.append("{}_error_{}_0".format(self.m_cur_date, k))
            self.m_feedback_source_vec.append("{}_error_{}_1".format(self.m_cur_date, k))

            code_0_key = "{}_error_{}_0".format(self.m_yes_date, k)
            code_1_key = "{}_error_{}_1".format(self.m_yes_date, k)

            if error_key_map[code_0_key] != "":
                code_0_num = int(error_key_map[code_0_key])
            else:
                code_0_num = 0

            if error_key_map[code_1_key] != "":
                code_1_num = int(error_key_map[code_1_key])
            else:
                code_1_num = 0

            total = code_0_num + code_1_num

            if total != 0:
                self.source_rate_map[k] = float(code_0_num) / total
            else:
                logger.error("source [{}] total num is 0".format(k))
                self.source_rate_map[k] = 1

        for k, v in source_summary_info.items():
            if 'summary' in k:
                _k_l = k.split('_')
                map_key = '_'.join([_k_l[1], _k_l[2], _k_l[3]])
                self.update_source_statistic_info(map_key, "total_tasks", v)
            elif 'Summary' in k:
                _k_l = k.split('_')
                map_key = '_'.join([_k_l[1], _k_l[2], _k_l[3]])
                self.insert_type_statistic_info(map_key, "total_tasks", k)

    def generate_task(self):
        if self.flight_task_queue.__len__() < MIN_FLIGHT_TASK_SIZE:
            self.add_flight_task_to_queue()

        if self.hotel_task_queue.__len__() < MIN_HOTEL_TASK_SIZE:
            pass

    def add_flight_task_to_queue(self):
        logger.info("[start add_flight_task]")
        assigned_task_num = 0
        assigned_task_num += self.append_task_from_retry('flight')
        self.append_task_from_package('flight', assigned_task_num)
        logger.info("[end add task][count: {}]".format(assigned_task_num))

    def adjust_source_frequency(self):
        error_key_map = {k: self.r.get(k) for k in self.m_feedback_source_vec}
        for k, v in self.source_rate_map.items():
            code_0_key = "{}_error_{}_0".format(self.m_cur_date, k)
            code_1_key = "{}_error_{}_0".format(self.m_cur_date, k)

            if error_key_map[code_0_key] != "":
                code_0_num = int(error_key_map[code_0_key])
            else:
                code_0_num = 0

            if error_key_map[code_1_key] != "":
                code_1_num = int(error_key_map[code_1_key])
            else:
                code_1_num = 0

            total = code_0_num + code_1_num
            if total == 0:
                continue

            rate = float(code_0_num) / total

            # todo unknown code
            # if rate < v * ADJUST_FREQUENCY_RATE:
            #     if it_s

    def assign_task(self, data_type, source, forbid_source, count, client_ip):
        tasks = self.get_task(data_type, source, forbid_source, client_ip)
        return "[{}]".format(','.join(map(lambda x: str(x), tasks)))

    def get_task(self, data_type, source, forbid_source, count, client_ip):
        types = data_type.split('_')
        if not types:
            return []

        return []

    def get_task_info_by_queue(self, _type, count):
        """
        get type_key and task
        :param _type:
        :param type_key:
        :param count:
        :return:
        """
        # todo get task func by queue
        # just pop
        task_info_list = []
        type_key = 'hotel'
        return [], type_key

    def append_get_task(self, task_info_list):
        tasks = []
        now = datetime.datetime.now()
        timeslot = ((now.hour * 60 + now.minute) / 5 + 1) % 288
        for each in task_info_list:
            t = Task()
            t.source = ''
            t.content = ''
            t.timeslot = timeslot
            tasks.append(t)
        return tasks

    def complete_task(self, task_str, client_ip):
        """

        :param task_str:
        [{"id": task.id, "content": task.content, "source": task.source,
           "workload_key": task.workload_key, "error": int(Error), 'proxy': "NULL",
           "timeslot": task.timeslot}]
        :param client_ip:
        :return:
        """
        logger.info("[complete task start]")
        for each_res in json.loads(task_str):
            workload_key = each_res['workload_key']
            t_type = self.get_task_type_by_source(each_res['source'])
            error_code = int(each_res['error'])

            # todo generate error key
            error_key = ''
            type_error_key = ''
            error_code_key = ''
            cur_retry_map = []  # 引用，之后修改

            self.increase_source_statistic_info(error_key, "completed_tasks")
            self.insert_type_statistic_info(error_key, "completed_tasks", type_error_key)
            self.increase_source_error_code_info_and_feedback(error_code_key, error_code)

            if error_code == 0:
                key = "{}_error_{}_0".format(self.m_cur_date, each_res['source'])
                self.increase_source_statistic_info(error_key, "completed_success_tasks")
                self.insert_type_statistic_info(error_key, "completed_success_tasks", type_error_key)
            else:
                key = "{}_error_{}_1".format(self.m_cur_date, each_res['source'])
                if error_code not in (99, 29):
                    # todo mk retry task
                    cur_retry_map[workload_key] = {
                        'workload_key': '',
                        'content_crawl': '',
                        'source': '',
                        'package_id': ''
                    }

            # todo 为什么要加 0 ？
            self.r.incr(key, 0)
            self.r.set(workload_key, datetime.datetime.now().strftime("%y-%m-%d 00:00:00|{}".format(error_code)))

        logger.info("[complete task end]")

    def get_content_from_rule(self):
        # todo 生成方式
        pass

    def append_task_from_retry(self, _tyoe):
        # todo 生成方式
        pass

    def append_task_from_package(self, _type, exist_num):
        # todo 生成方式
        priority_task_map = defaultdict(list)
        # todo max size -exist_num
        max_num = 10000 - exist_num


    def get_task_type_by_source(self, source):
        return ''

    def add_xxx_task_to_queue(self):
        pass

    def statistic_task_monitor(self):
        logger.info("[statistic task monitor start]")
        # todo 用于更新 task_monitor_summary 的绿皮显示
        # 使用 source_statistic_info 以及 type_statistic_info

    def update_source_statistic_info(self, first_key, sec_key, val):
        self.source_statistic_info[first_key, sec_key] = val

    def insert_type_statistic_info(self, first_key, sec_key, val):
        self.type_statistic_info[first_key, sec_key] = val

    def increase_source_statistic_info(self, first_key, sec_key):
        self.source_statistic_info[first_key, sec_key] += 1

    def increase_source_error_code_info_and_feedback(self, key, error_code):
        self.source_error_code_info[key, error_code] += 1
        self.feedback_info["S{}s".format(key), error_code] += 1


if __name__ == '__main__':
    BaseDistribution()
