#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午7:23
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertDateTask.py
# @Software: PyCharm
import math
import mock
import random
import pymongo
import datetime
import toolbox.Date
import common.patched_mongo_insert
from model.TaskType import TaskType
from logger import get_logger
from conf import config, task_source
from model.PackageInfo import PackageInfo
from toolbox.Date import date_takes
from model.DateTask import DateTask

toolbox.Date.DATE_FORMAT = '%Y%m%d'

INSERT_WHEN = 2000


class DateTaskList(list):
    def append_task(self, task: DateTask):
        self.append(task.to_dict())


class InsertDateTask(object):
    def __init__(self, task_type: TaskType):
        self.task_type = task_type

        # logger 记录日志
        self.logger = get_logger("InsertBaseTask")

        client = pymongo.MongoClient(host=config.mongo_host)
        self.base_task_db = client[config.mongo_base_task_db]
        self.date_task_db = client[config.mongo_date_task_db]

        self.base_collections = self.base_task_db[self.generate_base_collections()]

        # 按源生成多个 collections, 多个 tasks 队列
        self.date_collections_dict = {}
        self.tasks_dict = {}
        for each_source in self.get_total_source():
            self.date_collections_dict[each_source] = self.date_task_db[
                self.generate_date_collections(source=each_source)]

            # 建立索引
            self.create_indexes(source=each_source)

            # 初始化任务队列
            self.tasks_dict[each_source] = DateTaskList()

        # 初始化 PackageInfo 类
        self.package_info = PackageInfo()

        # 数据游标偏移量，用于在查询时发生异常恢复游标位置
        self.offset = 0
        # 数据游标前置偏移量，用于在入库时恢复游标位置
        self.pre_offset = 0

    def generate_base_collections(self):
        return "BaseTask_{}".format(str(self.task_type).split('.')[-1].title())

    @staticmethod
    def today():
        return datetime.datetime.today().strftime('%Y%m%d')

    def generate_date_collections(self, source):
        return "DateTask_{}_{}_{}".format(
            str(self.task_type).split('.')[-1].title(),
            source,
            self.today()
        )

    def create_indexes(self, source):
        collections = self.date_collections_dict[source]
        collections.create_index([('package_id', 1)])
        collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    @staticmethod
    def generate_package_start_and_part_num(task_n, split_n):
        """
            任务偏移量通过 2017-01-01 到今天的偏移，取任务天数进行计算，
            由此可以免去记录任务偏移量的内容，仅需要提供开始偏移以及最终偏移，
            以此简化数据存储
            :type task_n: int
            :type split_n: int
            :returns start_n int , part_num int
        """
        # type: int, int -> (int,int)
        # 通过日期偏移获取今天的 offset
        day_offset = (datetime.datetime.today() - datetime.datetime(2017, 1, 1)).days % split_n

        # 生成每部分的大小
        part_num = int(math.ceil(float(task_n) / float(split_n)))

        # self.task_seek
        start_n = int(part_num * day_offset)
        return start_n, part_num

    def get_total_source(self):
        if self.task_type == TaskType.flight:
            total_source = task_source.flight_source
        elif self.task_type == TaskType.round_flight:
            total_source = task_source.round_flight_source
        elif self.task_type == TaskType.multi_flight:
            total_source = task_source.multi_flight_source
        # todo add other source
        return total_source

    def generate_source(self, each_data):
        if self.task_type == TaskType.flight:
            source = random.choice(task_source.flight_source)
        elif self.task_type == TaskType.round_flight:
            source = random.choice(task_source.round_flight_source)
        elif self.task_type == TaskType.multi_flight:
            source = random.choice(task_source.multi_flight_source)
        # todo add other source
        return source

    def generate_date_task(self, package_id, each_data, date):
        """
        用于生成带有日期的任务
        :param package_id:
        :param each_data: dict 对象，其中存放着当前任务的具体信息
        :param date: task 的日期
        :return:
        """
        # type: package_id, dict, str -> DateTask
        if self.task_type == TaskType.flight:
            # 飞机需要拼接 date
            content = each_data['task_args']['content']
            content = "{}{}".format(content, date)

            date_task = DateTask(
                source=self.generate_source(each_data),
                package_id=package_id,
                task_type=self.task_type,
                date=date,
                content=content
            )
        # elif self.task_type == TaskType.round_flight:
        #     pass
        return date_task

    def mongo_patched_insert(self, data, source):
        collections = self.date_collections_dict[source]
        with mock.patch(
                'pymongo.collection.Collection._insert',
                common.patched_mongo_insert.Collection._insert
        ):
            result = collections.insert(data, continue_on_error=True)
            return result

    def __insert_mongo(self, source):
        if len(self.tasks_dict[source]) > 0:
            res = self.mongo_patched_insert(self.tasks_dict[source], source=source)
            self.logger.info(
                "[update offset][source: {}][offset: {}][pre offset: {}]".format(source, self.offset, self.pre_offset))
            self.offset = self.pre_offset
            self.logger.info("[insert info][source: {}][ offset: {} ][ {} ]".format(source, self.offset, res))
            self.logger.info('[ 本次准备入库任务数：{0} ][ 实际入库数：{1} ][ 库中已有任务：{2} ][ 已完成总数：{3} ]'.format(
                self.tasks_dict[source].__len__(), res['n'], res.get('err', 0), self.offset))
            # 入库完成，清空任务列表
            self.tasks_dict[source] = DateTaskList()

    def insert_mongo(self, source=None):
        if source is None:
            for each_source in self.tasks_dict.keys():
                self.__insert_mongo(source=each_source)
        else:
            self.__insert_mongo(source=source)

    def insert_stat(self, source):
        """
        用于检查当前是否可以准备将任务入到 mysql 中
        :return: bool 是否准备将任务入到 mysql 中
        """
        return len(self.tasks_dict[source]) >= INSERT_WHEN

    def _insert_task(self, date_task: DateTask):
        """
        插入一条 DateTask
        :param date_task:
        :return:
        """
        if isinstance(date_task, DateTask):
            self.tasks_dict[date_task.source].append_task(date_task)
            self.pre_offset += 1

            # 如果当前可以入库，则执行入库
            if self.insert_stat(source=date_task.source):
                self.insert_mongo(source=date_task.source)
        else:
            raise TypeError('错误的 args 类型 < {0} >'.format(type(date_task).__name__))

    def insert_task(self):
        # 获取 package_id 列表
        package_id_list = self.package_info.get_package()[self.task_type]
        for each_package_obj in package_id_list:
            # 基础任务请求
            task_query = {
                'task_type': self.task_type,
                'package_id': each_package_obj.package_id
            }

            # 基础任务计数
            _task_n = self.base_collections.count(task_query)

            # 本次任务生成份数
            _split_n = int(each_package_obj.update_cycle / 24)

            # 获取 start 位置以及生成份数
            start_n, part_num = self.generate_package_start_and_part_num(
                task_n=_task_n,
                split_n=_split_n
            )

            # 生成全量的 date 列表
            date_list = list(date_takes(each_package_obj.end_date - each_package_obj.start_date,
                                        ignore_days=each_package_obj.start_date))

            # 生成相应的日期任务
            for line in self.base_collections.find(
                    {
                        'task_type': self.task_type,
                        'package_id': each_package_obj.package_id
                    }
            ).sort([("_id", 1)]).skip(start_n).limit(part_num):
                # 遍历当前 package_id 下的所有任务
                for date in date_list:
                    # 遍历当前应该有的所有日期

                    # 生成新的日期任务
                    date_task = self.generate_date_task(
                        package_id=each_package_obj.package_id,
                        each_data=line,
                        date=date
                    )

                    # 插入新的日期任务
                    self._insert_task(date_task)

        # 最终入库，确保最后一部分数据能够入库
        self.insert_mongo()


if __name__ == '__main__':
    insert_date_task = InsertDateTask(task_type=TaskType.flight)
    insert_date_task.insert_task()
