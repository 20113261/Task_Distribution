#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:43
# @Author  : Hou Rong
# @Site    : 
# @File    : BaseTask.py
# @Software: PyCharm
import logging
import copy
from toolbox.Hash import get_token
from model.TaskType import TaskType

logger = logging.getLogger("MasterTask")


class BaseTask(object):
    def __init__(self, source, package_id, task_type: TaskType, **kwargs):
        # 任务基础信息
        self.source = source
        self.package_id = package_id
        self.task_type = task_type

        self.task_args = {
            'source': self.source,
            'content': kwargs.get('content', ''),
            'ticket_info': kwargs.get('ticket_info', {})
        }
        if task_type == TaskType.round_flight:
            self.task_args['continent_id'] = kwargs['continent_id']

        # 任务状态信息
        self.is_new_task = kwargs.get('is_new_task', False)
        self.has_update_date = kwargs.get('has_update_date', False)

        # 生成任务 id
        self.tid = self.generate_tid()

    def generate_tid(self):
        if self.task_type in (TaskType.flight, TaskType.round_flight):
            # 飞机列表页任务，每个源只发一个任务
            tmp_args = copy.deepcopy(self.task_args)
            if 'source' in tmp_args:
                tmp_args.pop('source')
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


if __name__ == '__main__':
    t = BaseTask('bookingListHotel')
    print(t.task_args, t.generate_tid())
    print(t.task_args, t.generate_tid())
    t.ticket_info['abc'] = 123123
    print(t.task_args, t.generate_tid())
    t2 = BaseTask('bookingListHotel', content="12312&a&20170901", ticket_info={'is_new_task': False})
    print(t2.task_args, t2.generate_tid())
