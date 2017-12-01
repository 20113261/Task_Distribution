#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午11:03
# @Author  : Hou Rong
# @Site    : 
# @File    : task_script.py
# @Software: PyCharm
import json
import time, uuid


class Task(object):
    """
    抓取任务
    """

    def __init__(self, **kwargs):
        # 增加任务创建时间，便于跟踪任务堆积
        self.create_time = time.time()
        self.new_task_id = str(uuid.uuid1())
        self.id = 0
        self.task_data = ''
        self.content = None  # 任务内容，不同source的格式可以不同，各个抓取下自行定义
        self.source = None  # 任务来源，用来表明谁来负责处理该任务
        self.task_type = 3  # 任务类型，1表示小时级任务，2表示天级任务，3表示长期任务
        self.priority = 0  # 优先级
        self.crawl_day = None  # 抓取时间，对应长期任务表示再该时间之前处理
        self.crawl_hour = 0  # 抓取时间（小时）
        self.update_times = 0  # 抓取次数
        self.success_times = 0  # 抓取成功次数
        self.update_time = None  # 更新时间
        self.proxy_info = {}  # 代理信息
        self.ticket_info = {}
        self.timeslot = -1

    def init_ticket_info(self, source, ticket_info):
        if not ticket_info:
            return

        source = source.lower()
        if 'flight' in source:
            ticket_info.setdefault('v_seat_type', 'E')
            count = int(ticket_info.setdefault('v_count', 2))
            ticket_info.setdefault('v_age', '_'.join(['-1'] * count))
            ticket_info.setdefault('v_hold_seat', '_'.join(['1'] * count))

        elif 'hotel' in source:
            ticket_info.setdefault('room_info', [])
            ticket_info.setdefault('occ', 2)
            ticket_info.setdefault('room_count', 1)

        elif 'train' in source:
            ticket_info.setdefault('v_seat_type', '2nd')
            count = int(ticket_info.setdefault('v_count', 2))
            ticket_info.setdefault('v_age', '_'.join(['-1'] * count))
            ticket_info.setdefault('v_hold_seat', '_'.join(['1'] * count))

        elif 'bus' in source:
            ticket_info.setdefault('v_seat_type', '2nd')
            count = int(ticket_info.setdefault('v_count', 2))
            ticket_info.setdefault('v_age', '_'.join(['-1'] * count))
            ticket_info.setdefault('v_hold_seat', '_'.join(['1'] * count))

        self.ticket_info.update(ticket_info)

    def __str__(self):
        return json.dumps(self.__dict__)

    def dict(self):
        return self.__dict__

    @staticmethod
    def parse(s):
        """
            从json字符串中解析初task
        """
        if s is None:
            return None

        if len(s.strip()) == 0:
            return None

        data = json.loads(s)

        if data is None:
            return None

        task = Task()
        for k, v in data.items():
            task.__dict__[k] = v

        task.task_data = s

        return task


if __name__ == '__main__':
    t = Task()
    t.source = 'demo'
    t.task_type = 'list'

    t2 = Task()
    t2.source = 'demo'
    t2.task_type = 'list'
    print("[{}]".format(','.join(map(lambda x: str(x), [t, t2]))))

    nt = Task.parse(
        '''{"proxy_info": {}, "new_task_id": "37e9df54-d596-11e7-a28a-4c32758b1219", "source": "demo", "update_time": null, "success_times": 0, "crawl_hour": 0, "create_time": 1512022681.797406, "task_data": "", "priority": 0, "crawl_day": null, "content": null, "timeslot": -1, "task_type": "list", "update_times": 0, "ticket_info": {}, "id": 0}''')

    for k, v in nt.__dict__.items():
        print(k, '->', v)

    print(nt.dict())

    nt = Task.parse('''{"content":"asdasdfasdf","ticket_info":{},"source":"demoListHotel"}''')

    for k, v in nt.dict().items():
        print(k, v)
