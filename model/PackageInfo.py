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
        self.package_id = package_id
        self.task_type = task_type
        self.update_cycle = update_cycle


class PackageInfo(object):
    def __init__(self):
        client = pymongo.MongoClient(host=config.mongo_host)
        self.collection = client[config.mongo_db][config.package_info_collection]

    @ttl_cache(maxsize=64, ttl=600)
    def get_package(self):
        _dict = defaultdict(list)
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
                _dict[task_type].append(package)
        return _dict


if __name__ == '__main__':
    package_info = PackageInfo()
    _dict = package_info.get_package()

    for k, v in _dict.items():
        print(k, '->', v)
