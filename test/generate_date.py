#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午10:35
# @Author  : Hou Rong
# @Site    : 
# @File    : generate_date.py
# @Software: PyCharm

import init_path
import math
import datetime
from model.TaskType import TaskType
from common.InsertDateTask import InsertDateTask
from common.InsertBaseTask import InsertBaseTask
from common.TempTask import TempTask

if __name__ == '__main__':
    insert_date_task = InsertDateTask(TaskType.Hotel, is_test=True)
    insert_date_task.insert_task()

    # TempTask.insert_task()
