#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午8:42
# @Author  : Hou Rong
# @Site    : 
# @File    : PackageInfo.py
# @Software: PyCharm
import pymongo
from conf import config
from cachetools.func import ttl_cache
from model.TaskType import TaskType
from collections import defaultdict


class PackageId(object):
    def __init__(self, package_id, task_type: TaskType, update_cycle):
        self.__package_id = -1
        self.__update_cycle = -1

        self.task_type = task_type
        self.package_id = package_id
        self.update_cycle = update_cycle

    @property
    def package_id(self):
        return self.__package_id

    @package_id.setter
    def package_id(self, val):
        self.__package_id = int(val)

    @property
    def update_cycle(self):
        return self.__update_cycle

    @update_cycle.setter
    def update_cycle(self, val):
        self.__update_cycle = int(val)

    def __eq__(self, other):
        return self.package_id == other.package_id and self.task_type == other.task_type

    def __lt__(self, other):
        if self.task_type == other.task_type:
            return self.package_id < other.package_id
        else:
            raise TypeError("[diff task type][ {} < {} ]".format(self.task_type, other.task_type))

    def __str__(self):
        return "[task_type: {}][pid: {}][update_cycle: {}]".format(self.task_type, self.package_id, self.update_cycle)

    def __repr__(self):
        return self.__str__()


class PackageInfo(object):
    def __init__(self):
        client = pymongo.MongoClient(host=config.mongo_host)
        self.collection = client[config.mongo_db][config.package_info_collection]

    @ttl_cache(maxsize=64, ttl=600)
    def get_package(self):
        __dict = defaultdict(list)
        for line in self.collection.find({}):
            package_id = int(line['id'])
            task_type = TaskType.parse_str(line['taskType'])
            update_cycle = line['update_cycle']
            if package_id >= 0:
                package = PackageId(
                    package_id=package_id,
                    task_type=task_type,
                    update_cycle=update_cycle
                )
                __dict[task_type].append(package)
        # sort dict
        for k in __dict.keys():
            __dict[k] = sorted(__dict[k])

        return __dict


if __name__ == '__main__':
    package_info = PackageInfo()
    _dict = package_info.get_package()

    for k, v in _dict.items():
        print(k, '->', v)
