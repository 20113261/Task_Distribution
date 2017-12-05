#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午5:46
# @Author  : Hou Rong
# @Site    : 
# @File    : DateList.py
# @Software: PyCharm
import pymongo
import datetime
from model.TaskType import TaskType
from conf import config


class TaskSeek(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host=config.mongo_host)
        self.collections = self.client[config.mongo_db]['TaskSeek']

        # 建立索引
        self.create_indexes()

    def create_indexes(self):
        self.collections.create_index([('task_type', 1), ("package_id", 1)], unique=True)

    @staticmethod
    def today():
        return datetime.datetime.today().strftime("%Y%m%d")

    @staticmethod
    def parse_date(date):
        return datetime.datetime.strptime(date, "%Y%m%d")

    def init_seek(self, task_type: TaskType, package_id: int):
        self.collections.save({
            'task_type': task_type,
            'package_id': package_id,
            'init_date': self.today()
        })

    def generate_date(self, task_type: TaskType, package_id: int):
        _res = self.collections.find_one({"task_type": task_type, "package_id": package_id})
        if not _res:
            pass
