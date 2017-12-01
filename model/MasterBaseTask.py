#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:43
# @Author  : Hou Rong
# @Site    : 
# @File    : MasterBaseTask.py
# @Software: PyCharm
import logging
from toolbox.Hash import get_token

logger = logging.getLogger("MasterTask")


class MasterBaseTask(object):
    def __init__(self, source, **kwargs):
        # 任务基础信息
        self.source = source
        self.content = kwargs.get('content', '')
        self.ticket_info = kwargs.get('ticket_info', {})

        self.task_args = {
            'source': self.source,
            'content': self.content,
            'ticket_info': self.ticket_info
        }

        # 任务状态信息
        self.is_new_task = kwargs.get('is_new_task', False)
        self.has_update_date = kwargs.get('has_update_date', False)

    def generate_tid(self):
        return get_token(self.task_args)


class MasterTask(MasterBaseTask):
    def __init__(self, source, date_index, **kwargs):
        super().__init__(source, **kwargs)
        self.date_index = date_index
        self.date = self.generate_date()

        self.task_args['date'] = self.date

    def generate_date(self):
        # todo generate date
        return self.date_index


if __name__ == '__main__':
    t = MasterBaseTask('bookingListHotel')
    print(t.task_args, t.generate_tid())
    print(t.task_args, t.generate_tid())
    t.ticket_info['abc'] = 123123
    print(t.task_args, t.generate_tid())
    t2 = MasterBaseTask('bookingListHotel', content="12312&a&20170901", ticket_info={'is_new_task': False})
    print(t2.task_args, t2.generate_tid())
