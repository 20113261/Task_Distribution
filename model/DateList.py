#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午5:46
# @Author  : Hou Rong
# @Site    : 
# @File    : TaskSeek.py
# @Software: PyCharm
import pymongo
import datetime
from model.TaskType import TaskType

TaskConfig = {
    # 每天更新一次
    (TaskType.flight, 0): 1,
    # 2 天一次
    (TaskType.flight, 1): 2,
    # 4 天一次
    (TaskType.flight, 2): 4,
    # 8 天一次
    (TaskType.flight, 3): 8,
    # 10 天一次
    (TaskType.flight, 4): 10,
    # (TaskType.flight, ): (0, 0),
    # (TaskType.flight, 0): (0, 0),
}


class TaskSeek(object):
    def __init__(self):
        self.client = pymongo.MongoClient(host='10.10.213.148')
        self.collections = self.client['RoutineBaseTask']['TaskSeek']

        # 建立索引
        self.create_indexes()

    def create_indexes(self):
        self.collections.create_index([('task_type', 1)], unique=True)

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

    def get_seek(self, task_type: TaskType):
        _res = self.collections.find_one({"task_type": task_type})
        if not _res:
            pass
