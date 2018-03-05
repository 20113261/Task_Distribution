#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午8:42
# @Author  : Hou Rong
# @Site    : 
# @File    : PackageInfo.py
# @Software: PyCharm
import pymongo
import json
from conf import config
from cachetools.func import ttl_cache
from model.TaskType import TaskType
from collections import defaultdict


class PackageId(object):
    def __init__(self, package_id, task_type: TaskType, update_cycle, start_date, end_date, slice_num, next_slice):
        self.package_id = int(package_id)
        self.update_cycle = int(update_cycle)
        self.start_date = int(start_date)
        self.end_date = int(end_date)

        self.task_type = task_type
        self.package_id = package_id
        self.update_cycle = update_cycle
        self.slice_num = slice_num
        self.next_slice = next_slice

    def __eq__(self, other):
        return self.package_id == other.package_id and self.task_type == other.task_type

    def __lt__(self, other):
        if self.task_type == other.task_type:
            return self.package_id < other.package_id
        else:
            raise TypeError("[diff task type][ {} < {} ]".format(self.task_type, other.task_type))

    def __str__(self):
        return json.dumps(self.__dict__, sort_keys=True)

    def __repr__(self):
        return self.__str__()


class PackageInfo(object):
    def __init__(self):
        client = pymongo.MongoClient(host=config.mongo_host)
        self.collection = client[config.mongo_base_task_db][config.package_info_collection]

    @ttl_cache(maxsize=64, ttl=600)
    def get_package(self) -> {int: [PackageId, ]}:
        __dict = defaultdict(list)
        for line in self.collection.find({}):
            package_id = int(line['id'])
            task_type = TaskType.parse_str(line['taskType'])
            update_cycle = line['update_cycle']
            if package_id >= 0:
                package = PackageId(
                    package_id=package_id,
                    task_type=task_type,
                    update_cycle=update_cycle,
                    start_date=line['daydiff_start'],
                    end_date=line['daydiff_end'],
                    slice_num=line['slice'],
                    next_slice=line.get('next_slice')
                )
                __dict[task_type].append(package)
        # sort dict
        for k in __dict.keys():
            __dict[k] = sorted(__dict[k])

        return __dict

    def alter_slice_num(self, package_obj: PackageId):
        #修改package_info里的sice值
        for line in self.collection.find({'id': package_obj.package_id}):
            total_slice_num = line['update_cycle'] / 24
        for line in self.collection.find({'id': package_obj.package_id}):
            current_slice_num = line['slice']
        slice_num = current_slice_num + 1 if current_slice_num < total_slice_num else current_slice_num
        if slice_num == total_slice_num:
            slice_num = 0
        self.collection.update({'id': package_obj.package_id}, {'$set': {'slice': slice_num}})

# if __name__ == '__main__':
#     package_info = PackageInfo()
#     _dict = package_info.get_package()
#
#     for k, v in _dict.items():
#         print(k, '->', v)
