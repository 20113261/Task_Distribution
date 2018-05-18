#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午7:23
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertDateTask.py
# @Software: PyCharm
'''此版本酒店为最新版需求'''

import math
import copy
import mock
import random
import pymongo
import datetime
import toolbox.Date
import init_path
from collections import defaultdict
import common.patched_mongo_insert
from model.TaskType import TaskType
from logger_file import get_logger
from conf import config, task_source
from model.PackageInfo import PackageInfo
from toolbox.Date import date_takes
from model.DateTask import DateTask
from logger_file import get_logger
from common.TempTask import TempTask
from common.generate_task_utils import sort_date_task_cursor, decide_need_hotel_task, today_date
from conf.config import multiply_times, frequency

logger = get_logger('InsertDateTask')
logger_2 = get_logger('Insert_process')

toolbox.Date.DATE_FORMAT = '%Y%m%d'

INSERT_WHEN = 2000


class DateTaskList(list):
    def append_task(self, task: DateTask):
        self.append(task.to_dict())


class InsertDateTask(object):
    def __init__(self, task_type: TaskType, number=None, routine=True, is_test=False):
        self.task_type = task_type

        self.number = number if number is not None else ''

        # logger 记录日志
        self.logger = get_logger("InsertBaseTask")

        client = pymongo.MongoClient(host=config.mongo_host)
        self.base_task_db = client[config.mongo_base_task_db]
        self.date_task_db = client[config.mongo_date_task_db]
        self.is_test = is_test
        if self.is_test:
            self.date_task_db = client['Test_RoutineDateTask']

        self.base_collections = self.base_task_db[self.generate_base_collections()]

        # 初始化 PackageInfo 类
        self.package_info = PackageInfo()

        #删除切片周期为1的mongo文档。
        if routine and is_test is False:
            self.delete_single_slice()

        # 按源生成多个 collections, 多个 tasks 队列
        self.date_collections_dict = {}
        self.tasks_dict = {}
        for each_source in self.get_total_source():
            self.date_collections_dict[each_source] = self.generate_date_collections(source=each_source)

            # 建立索引
            self.create_indexes(source=each_source)

            # 初始化任务队列
            self.tasks_dict[each_source] = DateTaskList()

        # 数据游标偏移量，用于在查询时发生异常恢复游标位置
        self.offset = 0
        # 数据游标前置偏移量，用于在入库时恢复游标位置
        self.pre_offset = 0
        # 用于遇到上一次无含早情况下，提示下一次再多取一次含早
        self.take_one_more = 0
        #
        self.memory_count_by_source = {}

    def delete_single_slice(self):
        if self.task_type == TaskType.Hotel:
            for collection_name in self.date_task_db.collection_names():
                for package_id in range(5, 9):
                    if frequency.get(str(package_id)) == 1:
                        self.date_task_db[collection_name].remove({'package_id': package_id})
        elif self.task_type == TaskType.Flight:
            for collection_name in self.date_task_db.collection_names():
                self.date_task_db[collection_name].remove({'package_id': 0})
        elif self.task_type == TaskType.RoundFlight:
            for collection_name in self.date_task_db.collection_names():
                self.date_task_db[collection_name].remove({'package_id': 12})
        #TempFlight的InsertDateTask任务不删除周期为1的数据，留在每天delete_task_supervise.py里删除。
        # elif self.task_type == TaskType.TempFlight:
        #     TempTask.delete_single_slice(self.package_info, self.date_task_db, self.task_type)


    def generate_base_collections(self):
        return "BaseTask_{}{}".format(str(self.task_type).split('.')[-1], self.number)


    def generate_date_collections(self, source):
        if self.task_type in [TaskType.TempFlight]:
            return "TemplateTask&{}_Flight_{}_{}".format(
                self.number,
                source,
                today_date(self.is_test)
            )
        elif self.task_type == TaskType.TempHotel:
            return "TemplateTask&{}_Hotel_{}_{}".format(
                self.number,
                source,
                today_date(self.is_test)
            )
        else:
            return "DateTask_{}_{}_{}".format(
                str(self.task_type).split('.')[-1],
                source,
                today_date(self.is_test)
            )

    def create_indexes(self, source):
        collections = self.date_task_db[self.date_collections_dict[source]]
        collections.create_index([('package_id', 1)])
        collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    # @staticmethod
    def generate_package_start_and_part_num(self, task_n, split_n, package_id):
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
        # day_offset = (datetime.datetime.today() - datetime.datetime(2017, 1, 2)).days % split_n
        day_offset_result = self.base_task_db[config.package_info_collection].find({'id': package_id})
        for line in day_offset_result:
            day_offset = line['slice']

        # 生成每部分的大小
        part_num = int(math.ceil(float(task_n) / float(split_n)))

        # self.task_seek
        start_n = int(part_num * day_offset)
        return start_n, part_num

    def get_total_source(self):
        total_source = TaskType.get_source_list(str(self.task_type).split('.')[-1])
        # todo add other source
        return total_source

    def generate_source(self):
        source_list = TaskType.get_source_list(str(self.task_type).split('.')[-1])
        source = random.choice(source_list)
        return source

    def generate_date_task(self, source, package_id, each_data, date, slice_num, collection_name):
        """
        用于生成带有日期的任务
        :param package_id:
        :param each_data: dict 对象，其中存放着当前任务的具体信息
        :param date: task 的日期
        :return:
        """
        # type: package_id, dict, str -> DateTask
        if self.task_type in [TaskType.Flight, TaskType.TempFlight]:
            # 飞机需要拼接 date
            content = each_data['task_args']['content']
            content = "{}{}".format(content, date)

            date_task = DateTask(
                source=source,
                package_id=package_id,
                task_type=self.task_type,
                date=date,
                content=content,
                slice_num=slice_num,
                collection_name=collection_name
            )
            yield date_task

        elif self.task_type == TaskType.RoundFlight:
            content = each_data['task_args']['content']
            continent_id = each_data['task_args']['continent_id']
            if int(continent_id) == 10:
                start_interval = 4
                end_interval = 8
                self.n1 += 1
            elif int(continent_id) == 20:
                start_interval = 9
                end_interval = 14
                self.n2 += 1

            elif int(continent_id) == 30:
                start_interval = 8
                end_interval = 13
                self.n3 += 1

            elif int(continent_id) == 40:
                start_interval = 10
                end_interval = 11
                self.n4 += 1

            elif int(continent_id) == 50:
                start_interval = 8
                end_interval = 15
                self.n5 += 1

            elif int(continent_id) == 60:
                self.n6 += 1
                start_interval = 10
                end_interval = 11

            date = datetime.datetime.strptime(date, '%Y%m%d')
            for i in range(start_interval, end_interval):
                # if int(continent_id) != 60:
                #     break
                round_date = date + datetime.timedelta(days=i)
                round_date = round_date.strftime('%Y%m%d')
                date_task = DateTask(
                    source=source,
                    package_id=package_id,
                    task_type=self.task_type,
                    date=datetime.datetime.strftime(date, '%Y%m%d') + '&' + round_date,
                    content=content,
                    continent_id=continent_id,
                    slice_num=slice_num,
                    collection_name=collection_name
                )
                yield date_task

        elif self.task_type == TaskType.MultiFlight:
            content = each_data['task_args']['content']
            continent_id = each_data['task_args']['continent_id']
            if int(continent_id) == 50:
                end_board = 12
            elif int(continent_id) == 20:
                end_board = 9
            elif int(continent_id) == 10:
                end_board = 4
            elif int(continent_id) == 30:
                end_board = 9
            date = datetime.datetime.strptime(date, '%Y%m%d')
            multi_date = date + datetime.timedelta(days=end_board)
            multi_date = multi_date.strftime('%Y%m%d')
            date_task = DateTask(
                source=source,
                package_id=package_id,
                task_type=self.task_type,
                date=datetime.datetime.strftime(date, '%Y%m%d') + '&' + multi_date,
                content=content,
                continent_id=continent_id,
                slice_num=slice_num,
                collection_name=collection_name
            )
            yield date_task

        elif self.task_type in [TaskType.Hotel, TaskType.TempHotel]:
            city_id = each_data['task_args']['city_id']
            suggest_type = each_data['task_args']['suggest_type']
            suggest = each_data['task_args']['suggest']
            country_id = each_data['task_args']['country_id']
            content = "{}&{}&{}&{}".format(city_id, 2, 1, date)
            date_task = DateTask(
                source=source,
                package_id=package_id,
                task_type=self.task_type,
                date=date,
                content=content,
                city_id=city_id,
                suggest=suggest,
                suggest_type=suggest_type,
                country_id=country_id,
                slice_num=slice_num,
                collection_name=collection_name
            )
            date_task_list = []
            date_task.task_args['ticket_info']['hotel_info']['bed_type'] = -1
            need_breakfast_column = each_data['need_breakfast_column']
            if need_breakfast_column == 0:
                date_task.task_args['ticket_info']['hotel_info']['has_breakfast'] = -1
                date_task.tid = date_task.generate_tid()
                date_task_list.append(copy.deepcopy(date_task))
            elif need_breakfast_column == 1:
                date_task.task_args['ticket_info']['hotel_info']['has_breakfast'] = 1
                date_task.tid = date_task.generate_tid()
                date_task_list.append(copy.deepcopy(date_task))
            else:
                date_task.task_args['ticket_info']['hotel_info']['has_breakfast'] = -1
                date_task.tid = date_task.generate_tid()
                date_task_list.append(copy.deepcopy(date_task))
                date_task.task_args['ticket_info']['hotel_info']['has_breakfast'] = 1
                date_task.tid = date_task.generate_tid()
                date_task_list.append(copy.deepcopy(date_task))

            yield from date_task_list

                # date_task.task_args['ticket_info']['bed_type'] = 1
                # date_task.tid = date_task.generate_tid()
                # date_task_list.append(copy.deepcopy(date_task))
                # date_task.task_args['ticket_info']['bed_type'] = 2
                # date_task.tid = date_task.generate_tid()
                # date_task_list.append(copy.deepcopy(date_task))
                # date_task.task_args['ticket_info']['bed_type'] = -1
                # date_task.task_args['ticket_info']['has_breakfast'] = 1
                # date_task.tid = date_task.generate_tid()
                # date_task_list.append(copy.deepcopy(date_task))

        elif self.task_type in [TaskType.Train]:
            content = each_data['task_args']['content']
            content = "{}&{}".format(content, date)

            date_task = DateTask(
                source=source,
                package_id=package_id,
                task_type=self.task_type,
                date=date,
                content=content,
                slice_num=slice_num,
                collection_name=collection_name
            )
            yield date_task

        elif self.task_type in [TaskType.Ferries]:
            content = each_data['task_args']['content']
            content = "{}&{}".format(content, date)

            date_task = DateTask(
                source=source,
                package_id=package_id,
                task_type=self.task_type,
                date=date,
                content=content,
                slice_num=slice_num,
                collection_name=collection_name
            )
            yield date_task

    def generate_task(self, each_package_obj, date_list, source, line):
        collection_name = self.date_collections_dict[source]

        count = self.count_source[each_package_obj.package_id].get(source, 0)
        self.count_source[each_package_obj.package_id][source] = count + 1

        for date in date_list:
            # 遍历当前应该有的所有日期

            # 生成新的日期任务
            date_task = self.generate_date_task(
                source,
                package_id=each_package_obj.package_id,
                each_data=line,
                date=date,
                slice_num=each_package_obj.slice_num,
                collection_name=collection_name
            )

            # 插入新的日期任务
            for i in date_task:
                self.n += 1
                logger_str = str(self.task_type) + str(self.n)
                # logger.info(logger_str)
                self._insert_task(i)

    def mongo_patched_insert(self, data, source):
        try:
            collections = self.date_task_db[self.date_collections_dict[source]]
            with mock.patch(
                    'pymongo.collection.Collection._insert',
                    common.patched_mongo_insert.Collection._insert
            ):
                result = collections.insert(data, continue_on_error=True)
                return result
        except Exception as e:
            logger.error("发生异常", exc_info=1)


    # def query_hotel_base(self, city_id):
    #     res = []
    #     cursor = []
    #     for line in self.base_collections.find({'task_args.city_id': city_id}):
    #         cursor.append(line)
    #     # random.shuffle(cursor)
    #     cursor = sort_date_task_cursor(cursor)
    #     query_count = len(cursor)
    #     take_count = 2 if query_count >= 2 else 1
    #     line_list = cursor[0:2]
    #     try:
    #         for line in line_list:
    #             if line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
    #                 line['need_breakfast_column'] = 0
    #                 res.append(line)
    #                 if query_count > take_count:
    #                     next_line = cursor[take_count]
    #                     take_count += 1
    #                     if next_line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
    #                         if query_count > take_count:
    #                             next_line = cursor[take_count]
    #                             next_line['need_breakfast_column'] = 1
    #                             res.append(next_line)
    #                             take_count += 1
    #                     else:
    #                         next_line['need_breakfast_column'] = 1
    #                         res.append(next_line)
    #             else:
    #                 line['need_breakfast_column'] = 2
    #                 res.append(line)
    #     except Exception as e:
    #         logger.info('{}{}'.format('+++++', cursor))
    #         raise e
    #     yield from res

    def query_hotel_base(self, city_id):
        res = []
        cursor = []
        for line in self.base_collections.find({'task_args.city_id': city_id}):
            cursor.append(line)
        cursor = sort_date_task_cursor(cursor)
        query_count = len(cursor)
        take_count = 2 if query_count >= 2 else 1

        try:
            for each_line in cursor[0:2]:
                package_id = each_line['package_id']
                source = each_line['task_args']['source']
                self.memory_count_by_source[source] = self.memory_count_by_source.get(source, 0) + multiply_times[str(package_id)]
                if self.memory_count_by_source[source] > 300000:
                    if query_count > take_count:
                        line, take_count = decide_need_hotel_task(cursor, take_count, self.memory_count_by_source)
                else:
                    line = each_line
                source = line['task_args']['source']
                if source in ['agodaListHotel', 'elongListHotel']:
                    line['need_breakfast_column'] = 0
                    res.append(line)
                    if query_count > take_count:
                        next_line, take_count = decide_need_hotel_task(cursor, take_count, self.memory_count_by_source)
                        if next_line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
                            if query_count > take_count:
                                next_line, take_count = decide_need_hotel_task(cursor, take_count, self.memory_count_by_source)
                                next_line['need_breakfast_column'] = 1
                                res.append(next_line)
                        else:
                            next_line['need_breakfast_column'] = 1
                            res.append(next_line)
                else:
                    line['need_breakfast_column'] = 2
                    res.append(line)

        except Exception as e:
            logger.info('{}{}'.format('+++++', cursor))
            logger.exception(msg='出错', exc_info=e)
            raise e
        yield from res


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

    # @is_hotel_type
    def insert_data(self, each_package_obj):
        # 基础任务请求
        try:
            if self.task_type in [TaskType.Hotel, TaskType.TempHotel]:
                _task_n = 0
                cursor = self.base_collections.aggregate(
                    [
                        {'$match': {'package_id': each_package_obj.package_id}},
                        {'$group': {'_id': "$task_args.city_id",
                                    'task': {'$push': "$$ROOT"}}}]
                )
                for i in cursor:
                    if len(i['task']) == 1:
                        _task_n += 1
                    else:
                        _task_n += 2
            else:
                task_query = {
                    'task_type': self.task_type,
                    'package_id': each_package_obj.package_id
                }

                # self.base_collections是BaseTask集合。 计算基础任务计数
                _task_n = self.base_collections.count(task_query)
        except Exception as e:
            logger.error("发生异常", exc_info=1)

        # 本次任务生成份数
        _split_n = int(each_package_obj.update_cycle / 24)

        # 获取 start 位置以及生成份数
        start_n, part_num = self.generate_package_start_and_part_num(
            task_n=_task_n,
            split_n=_split_n,
            package_id=each_package_obj.package_id
        )

        # 生成全量的 date 列表
        date_list = list(date_takes(each_package_obj.end_date - each_package_obj.start_date,
                                    ignore_days=each_package_obj.start_date))

        try:
            if self.task_type in [TaskType.Hotel, TaskType.TempHotel]:
                start_index = 0
                # for city_id in self.base_collections.distinct('task_args.city_id', {'package_id': each_package_obj.package_id}):
                cursor = self.base_collections.aggregate(
                    [
                        {'$match': {'package_id': each_package_obj.package_id}},
                        {'$group': {'_id': "$task_args.city_id",
                                    'task': {'$push': "$$ROOT"}}}]
                )
                for i in cursor:
                    if len(i['task']) == 1:
                        start_index += 1
                    else:
                        start_index += 2
                    if start_index <= start_n:
                        continue
                    if start_index > (start_n + part_num):
                        break
                    for line in self.query_hotel_base(i['task'][0]['task_args']['city_id']):
                        source = line['task_args']['source']
                        self.generate_task(each_package_obj, date_list, source, line)
            else:
                query = {
                        'task_type': self.task_type,
                        'package_id': each_package_obj.package_id
                    }
                # 在BaseTask集合中查找符合task_type的记录
                for line in self.base_collections.find(query).sort([("_id", 1)]).skip(start_n).limit(part_num):
                    # if line['task_args']['continent_id'] in ['10']:
                    #     self.continent += 1
                    #火车固定source,飞机随机性选source
                    if self.task_type in [TaskType.Train]:
                        source = 'raileuropeApiRail'
                    else:
                        source = self.generate_source()
                    self.generate_task(each_package_obj, date_list, source, line)
        except Exception as e:
            logger.error("发生异常", exc_info=1)

        # 最终入库，确保最后一部分数据能够入库
        self.insert_mongo()
        logger.info(self.count_source)


    def insert_task(self):
        self.n = 0
        self.n1 = 0
        self.n2 = 0
        self.n3 = 0
        self.n4 = 0
        self.n5 = 0
        self.n6 = 0
        self.continent = 0
        self.count_source = defaultdict(dict)

        # 获取 package_id 列表
        package_id_list = self.package_info.get_package()[self.task_type]
        #package_id_list是package_info集合中符合task_type的记录列表
        for each_package_obj in package_id_list:
            if each_package_obj.package_id in [100]:
                continue
            if each_package_obj.next_slice == 0:
                continue
            self.insert_data(each_package_obj)

            self.package_info.alter_slice_num(each_package_obj)
        logger.info(self.memory_count_by_source)


if __name__ == '__main__':
    #如果不能删除现有的周期为1的数据，则此main函数不能执行。
    logger_2.info('酒店：')
    insert_date_task = InsertDateTask(task_type=TaskType.Hotel)
    insert_date_task.insert_task()
    logger_2.info('联程飞机：')
    insert_date_task = InsertDateTask(task_type=TaskType.MultiFlight)
    insert_date_task.insert_task()
    logger_2.info('单程飞机：')
    insert_date_task = InsertDateTask(task_type=TaskType.Flight)
    insert_date_task.insert_task()

    logger_2.info('往返飞机：')
    insert_date_task = InsertDateTask(task_type=TaskType.RoundFlight)
    insert_date_task.insert_task()
    logger_2.info('火车：')
    insert_date_task = InsertDateTask(task_type=TaskType.Train)
    insert_date_task.insert_task()
    logger_2.info('渡轮：')
    insert_date_task = InsertDateTask(task_type=TaskType.Ferries)
    insert_date_task.insert_task()
    logger_2.info('临时任务：')
    TempTask.insert_task()


