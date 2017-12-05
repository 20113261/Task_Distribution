#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:51
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertBaseTask.py
# @Software: PyCharm
import pymongo
import mock
import common.patched_mongo_insert
from model.TaskType import TaskType
from logger import get_logger
from model.MasterBaseTask import MasterBaseTask
from common.generate_task_info import generate_hotel_base_task_info, generate_flight_base_task_info, \
    generate_round_flight_base_task_info

INSERT_WHEN = 2000


class BaseTaskList(list):
    def append_task(self, task: MasterBaseTask):
        self.append(task.to_dict())


class InsertBaseTask(object):
    def __init__(self, task_type: TaskType):
        # 任务类型
        self.task_type = task_type

        # 初始化 collections 名称
        self.collection_name = self.generate_collection_name()

        # logger 记录日志
        self.logger = get_logger("InsertBaseTask")

        # 数据游标偏移量，用于在查询时发生异常恢复游标位置
        self.offset = 0
        # 数据游标前置偏移量，用于在入库时恢复游标位置
        self.pre_offset = 0

        client = pymongo.MongoClient(host='10.10.231.105')
        self.db = client['RoutineBaseTask']

        self.tasks = BaseTaskList()

        # 初始化建立索引
        self.create_mongo_indexes()

    def create_mongo_indexes(self):
        collections = self.db[self.collection_name]
        collections.create_index([('package_id', 1)])
        collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    def generate_collection_name(self):
        return "BaseTask_{}".format(str(self.task_type).split('.')[-1].title())

    def mongo_patched_insert(self, data):
        collections = self.db[self.collection_name]
        with mock.patch(
                'pymongo.collection.Collection._insert',
                common.patched_mongo_insert.Collection._insert
        ):
            result = collections.insert(data, continue_on_error=True)
            return result

    def insert_mongo(self):
        if len(self.tasks) > 0:
            res = self.mongo_patched_insert(self.tasks)
            self.logger.info("[update offset][offset: {}][pre offset: {}]".format(self.offset, self.pre_offset))
            self.offset = self.pre_offset
            self.logger.info("[insert info][ offset: {} ][ {} ]".format(self.offset, res))
            self.logger.info('[ 本次准备入库任务数：{0} ][ 实际入库数：{1} ][ 库中已有任务：{2} ][ 已完成总数：{3} ]'.format(
                self.tasks.__len__(), res['n'], res.get('err', 0), self.offset))

            # 入库完成，清空任务列表
            self.tasks = BaseTaskList()

    def insert_stat(self):
        """
        用于检查当前是否可以准备将任务入到 mysql 中
        :return: bool 是否准备将任务入到 mysql 中
        """
        return len(self.tasks) >= INSERT_WHEN

    def _insert_task(self, args: dict):
        if isinstance(args, dict):
            __t = MasterBaseTask(**args)
            self.tasks.append_task(__t)
            self.pre_offset += 1

            # 如果当前可以入库，则执行入库
            if self.insert_stat():
                self.insert_mongo()
        else:
            raise TypeError('错误的 args 类型 < {0} >'.format(type(args).__name__))

    def insert_task(self):
        if self.task_type == TaskType.flight:
            for dept, dest, package_id, source in generate_flight_base_task_info():
                content = "{}&{}&".format(dept, dest)
                self._insert_task(
                    {
                        'content': content,
                        'package_id': package_id,
                        'source': source,
                        'task_type': self.task_type
                    }
                )
        elif self.task_type == TaskType.round_flight:
            for dept, dest, package_id, source, continent_id in generate_round_flight_base_task_info():
                content = "{}&{}&".format(dept, dest)
                self._insert_task(
                    {
                        'content': content,
                        'package_id': package_id,
                        'source': source,
                        'task_type': self.task_type,
                        'continent_id': continent_id
                    }
                )
        elif self.task_type == TaskType.hotel:
            pass
        # 最终剩余数据入库
        if len(self.tasks) > 0:
            self.insert_mongo()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.insert_mongo()


if __name__ == '__main__':
    # with InsertBaseTask(task_type=TaskType.flight) as insert_task:
    #     insert_task.insert_task()
    insert_task = InsertBaseTask(task_type=TaskType.round_flight)
    insert_task.insert_task()
