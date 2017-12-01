#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午7:07
# @Author  : Hou Rong
# @Site    : 
# @File    : DateGenerator.py
# @Software: PyCharm


class DateGenerator(object):
    def generate_date_list(self):
        collection_name = "CityTaskDate"
        collections = self.db[collection_name]
        _res = collections.find_one({
            'task_name': self.task_name
        })
        if not _res:
            # 之后的 360 天
            dates = list(date_takes(90, 1, 3))
            # 随机排序
            random.shuffle(dates)

            date_obj_id = collections.save({
                'task_name': self.task_name,
                'dates': dates
            })
            self.logger.info("[new date list][task_name: {}][dates: {}]".format(self.task_name, dates))
        else:
            date_obj_id = _res['_id']
            self.logger.info(
                "[date already generate][task_name: {}][dates: {}]".format(_res['task_name'], _res['dates']))
        return date_obj_id
