#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午7:23
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertDateTask.py
# @Software: PyCharm
import pymongo
import datetime
from model.TaskType import TaskType
from logger import get_logger
from conf import config
from model.PackageInfo import PackageInfo


class InsertDateTask(object):
    def __init__(self, task_type: TaskType):
        self.task_type = task_type

        # logger 记录日志
        self.logger = get_logger("InsertBaseTask")

        client = pymongo.MongoClient(host=config.mongo_host)
        self.db = client[config.mongo_db]

        self.base_collections = self.db[self.generate_base_collections()]
        self.date_collections = self.db[self.generate_date_collections()]

        # 建立索引
        self.create_indexes()

        # 初始化 PackageInfo 类
        self.package_info = PackageInfo()

    def generate_base_collections(self):
        return "BaseTask_{}".format(str(self.task_type).split('.')[-1].title())

    def generate_date_collections(self):
        return "DateTask_{}".format(str(self.task_type).split('.')[-1].title())

    def create_indexes(self):
        collections = self.date_collections
        collections.create_index([('package_id', 1)])
        collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    def generate_package_start_end(self, task_n, split_n):
        """
            任务偏移量通过 2017-01-01 到今天的偏移，取任务天数进行计算，
            由此可以免去记录任务偏移量的内容，仅需要提供开始偏移以及最终偏移，
            以此简化数据存储
            :type task_n: int
            :type split_n: int
        """
        datetime.datetime.today()-datetime.datetime.strptime()
        # self.task_seek

    def insert_task(self):
        # 获取 package_id 列表
        package_id_list = self.package_info.get_package()[self.task_type]
        if self.task_type == TaskType.flight:
            for each_package_id in package_id_list:
                # 基础任务请求
                task_query = {
                    'task_type': self.task_type,
                    'package_id': each_package_id.package_id
                }

                # 基础任务计数
                _task_n = self.base_collections.count(task_query)

                # 本次任务生成份数
                _split_n = int(each_package_id.update_cycle / 24)

                # 生成相应的日期任务
                for line in self.base_collections.find(
                        {
                            'task_type': self.task_type,
                            'package_id': each_package_id.package_id
                        }
                ).sort([("_id", 1)]).skip():
                    pass


if __name__ == '__main__':
    insert_date_task = InsertDateTask(task_type=TaskType.flight)
    insert_date_task.insert_task()
