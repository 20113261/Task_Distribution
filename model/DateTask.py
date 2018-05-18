#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:43
# @Author  : Hou Rong
# @Site    : 
# @File    : DateTask.py
# @Software: PyCharm
import copy
import datetime
import json
from toolbox.Hash import get_token
from model.TaskType import TaskType
from collections import defaultdict


class DateTask(object):
    def __init__(self, source, package_id, task_type: TaskType, date, **kwargs):
        # 任务基础信息
        self.source = source
        self.package_id = package_id
        self.task_type = task_type
        self.date = date
        self.collection_name = kwargs['collection_name']
        self.task_args = {
            'source': self.source,
            'content': kwargs.get('content', ''),
            'ticket_info': kwargs.get('ticket_info', defaultdict(dict)),
            'date': date
        }
        if task_type == TaskType.RoundFlight:
            self.task_args['continent_id'] = kwargs['continent_id']

        elif task_type in [TaskType.Hotel, TaskType.TempHotel]:
            self.task_args['ticket_info']['hotel_info'] = defaultdict(dict)
            self.task_args['suggest'] = kwargs['suggest']
            self.task_args['suggest_type'] = kwargs['suggest_type']
            self.task_args['country_id'] = kwargs['country_id']
            self.task_args['ticket_info']['hotel_info']['city_id'] = kwargs['city_id']
            self.task_args['ticket_info']['hotel_info']['checkin'] = date
            checkin = datetime.datetime.strptime(date, "%Y%m%d")
            checkout = (checkin + datetime.timedelta(days=1)).strftime('%Y%m%d')
            self.task_args['ticket_info']['hotel_info']['checkout'] = checkout

            if self.package_id == 5:
                self.task_args['ticket_info']['hotel_info']['page_num'] = 20
            elif self.package_id == 6:
                self.task_args['ticket_info']['hotel_info']['page_num'] = 20
            elif self.package_id == 7:
                self.task_args['ticket_info']['hotel_info']['page_num'] = 10
            elif self.package_id == 8:
                self.task_args['ticket_info']['hotel_info']['page_num'] = 5

        elif task_type in [TaskType.Train]:
            self.task_args['ticket_info'] = {
                "v_count": 1,
                "auth": json.dumps({
                    "key": "8780929146871554943",
                    "url": "https://webservicesx.euronet.vsct.fr/V10/webservices/xml"
                })}
        elif task_type in [TaskType.Ferries]:
            self.task_args['ticket_info'] = {
                'car': {"vTId": 0, "vHId": 339, "vLId": 876},
                'psg': {'psgNo': 1, 'psgAge': [18]}
            }
        # 任务是否已完成
        self.finished = 0
        # 任务使用的次数
        self.used_times = 0

        # 生成任务 id
        if task_type not in [TaskType.Hotel, TaskType.TempHotel]:
            self.tid = self.generate_tid()
        # 判断数据入队状态，0：不在队列里了（未进入队列或已被爬虫反馈），1：在消息队列里
        self.run = 0
        #切片记录
        self.slice_num = kwargs['slice_num']
        #更新時間
        self.update_time = datetime.datetime.now()
        #取走次數
        self.take_times = 0
        #反馈次数
        self.feedback_times = 0

    def generate_tid(self):
        # if self.task_type in (TaskType.Flight, TaskType.RoundFlight, TaskType.MultiFlight, TaskType.Hotel, TaskType.TempFlight, TaskType.TempHotel, TaskType.Train):
        # 飞机列表页任务，每个源只发一个任务
        tmp_args = copy.deepcopy(self.task_args)
        # if 'source' in tmp_args:
        #     tmp_args.pop('source')#无解
        return get_token(tmp_args)

    @staticmethod
    def ignore_key():
        return ['content', 'ticket_info']

    def to_dict(self):
        tmp_res = copy.deepcopy(self.__dict__)
        for key in self.ignore_key():
            if hasattr(tmp_res, key):
                tmp_res.__delattr__(key)
        return tmp_res
