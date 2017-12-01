#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:51
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertBaseTask.py
# @Software: PyCharm
import pymongo
from model.MasterBaseTask import MasterTask
from model.TaskType import TaskType
from task_script.generate_task_info import generate_flight_base_task_info


class InsertBaseTask(object):
    def __init__(self, source, task_type, package_id, **kwargs):
        # 任务基本信息
        self.source = source
        self.task_type = task_type
        self.package_id = package_id

        self.collection_name = self.generate_collection_name()

        # 数据游标偏移量，用于在查询时发生异常恢复游标位置
        self.offset = 0
        # 数据游标前置偏移量，用于在入库时恢复游标位置
        self.pre_offset = 0

        client = pymongo.MongoClient(host='10.10.231.105')
        self.db = client['MongoTask']

        # 建立所需要的全部索引
        self.create_mongo_indexes()

        # CITY TASK 获取 date_list
        if self.task_type == TaskType.fligh:
            self.date_list = self.generate_list_date()
        else:
            self.date_list = None

        # 修改 logger 日志打印
        # modify handler's formatter
        datefmt = "%Y-%m-%d %H:%M:%S"
        file_log_format = "%(asctime)-15s %(threadName)s %(filename)s:%(lineno)d %(levelname)s " \
                          "[source: {}][type: {}][task_name: {}][collection_name: {}]:        %(message)s".format(
            self.source, self.type, self.task_name, self.collection_name)
        formtter = logging.Formatter(file_log_format, datefmt)

        for each_handler in self.logger.handlers:
            each_handler.setFormatter(formtter)
        self.logger.info("[init InsertTask]")

    def generate_collection_name(self):
        if self.task_type != TaskType.CITY_TASK:
            return "Task_Queue_{}_TaskName_{}".format(self.queue, self.task_name)
        else:
            return "City_Queue_{}_TaskName_{}".format(self.queue, self.task_name)

    def create_mongo_indexes(self):
        collections = self.db[self.collection_name]
        collections.create_index([('finished', 1)])
        collections.create_index([('priority', -1), ('used_times', 1), ('utime', 1)])
        collections.create_index([('queue', 1), ('finished', 1), ('running', 1)])
        collections.create_index([('queue', 1), ('finished', 1), ('running', 1), ('used_times', 1)])
        collections.create_index([('queue', 1), ('finished', 1), ('used_times', 1), ('priority', 1)])
        collections.create_index(
            [('queue', 1), ('finished', 1), ('used_times', 1), ('priority', 1), ('running', 1)],
            name='qyery_index')
        collections.create_index([('running', 1)])
        collections.create_index([('running', 1), ('utime', 1)])
        collections.create_index([('running', 1), ('utime', -1)])
        collections.create_index([('task_name', 1)])
        collections.create_index([('task_name', 1), ('finished', 1)])
        collections.create_index([('task_name', 1), ('finished', 1), ('used_times', 1)])
        collections.create_index([('task_name', 1), ('list_task_token', 1)])
        if self.task_type in (TaskType.NORMAL, TaskType.LIST_TASK):
            collections.create_index([('task_token', 1)], unique=True)
        elif self.task_type == TaskType.CITY_TASK:
            collections.create_index([('list_task_token', 1)], unique=True)
        collections.create_index([('utime', 1)])
        collections.create_index([('finished', 1)])
        self.logger.info("[完成索引建立]")

    def generate_list_date(self):
        collection_name = "CityTaskDate"
        collections = self.db[collection_name]
        _res = collections.find_one({
            'task_name': self.task_name
        })
        if not _res:
            dates = list(date_takes(360, 5, 10))
            collections.save({
                'task_name': self.task_name,
                'dates': dates
            })
            self.logger.info("[new date list][task_name: {}][dates: {}]".format(self.task_name, dates))
        else:
            self.logger.info(
                "[date already generate][task_name: {}][dates: {}]".format(_res['task_name'], _res['dates']))
        return _res['_id']

    def mongo_patched_insert(self, data):
        collections = self.db[self.collection_name]
        with mock.patch('pymongo.collection.Collection._insert', patched_mongo_insert.Collection._insert):
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
            self.tasks = TaskList()

    def insert_stat(self):
        """
        用于检查当前是否可以准备将任务入到 mysql 中
        :return: bool 是否准备将任务入到 mysql 中
        """
        return len(self.tasks) >= INSERT_WHEN

    def get_task(self):
        yield

    def insert_task(self, args):
        if isinstance(args, dict):
            __t = Task(worker=self.worker, source=self.source, _type=self.type, task_name=self.task_name,
                       routine_key=self.routine_key,
                       queue=self.queue, task_type=self.task_type, date_list=self.date_list, _args=args)
            self.tasks.append_task(__t)
            self.pre_offset += 1

            # 如果当前可以入库，则执行入库
            if self.insert_stat():
                self.insert_mongo()
        else:
            raise TypeError('错误的 args 类型 < {0} >'.format(type(args).__name__))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.insert_mongo()
