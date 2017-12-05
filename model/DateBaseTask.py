#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午5:41
# @Author  : Hou Rong
# @Site    : 
# @File    : DateBaseTask.py
# @Software: PyCharm
from model.TaskType import TaskType


class DateBaseTask(object):
    def __init__(self, task_type: TaskType, master_task_id):

