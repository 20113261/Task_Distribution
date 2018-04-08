#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午10:35
# @Author  : Hou Rong
# @Site    : 
# @File    : generate_date.py
# @Software: PyCharm
import math
import datetime
from model.TaskType import TaskType
from common.InsertDateTask import InsertDateTask
from common.InsertBaseTask import InsertBaseTask

if __name__ == '__main__':
    insert_date_task = InsertDateTask(TaskType.Ferries, is_test=True)
    insert_date_task.insert_task()

