#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午7:23
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertDateTask.py
# @Software: PyCharm
import pymongo
from model.TaskType import TaskType
from logger import get_logger
from conf import config


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

    def generate_base_collections(self):
        return "BaseTask_{}".format(str(self.task_type).split('.')[-1].title())

    def generate_date_collections(self):
        return "DateTask_{}".format(str(self.task_type).split('.')[-1].title())

    def create_indexes(self):
        collections = self.date_collections
        collections.create_index([('package_id', 1)])
        collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    def insert_task(self):
        if self.task_type == TaskType.flight:
            for line in self.base_collections.find({'task_type': self.task_type}):
                pass


if __name__ == '__main__':
    insert_date_task = InsertDateTask(task_type=TaskType.flight)
    insert_date_task.insert_task()
