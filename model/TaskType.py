#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午6:50
# @Author  : Hou Rong
# @Site    : 
# @File    : TaskType.py
# @Software: PyCharm
import enum


class TaskType(enum.IntEnum):
    flight = 0
    round_flight = 1
    multi_flight = 2
    hotel = 3


if __name__ == '__main__':
    print(TaskType.flight)
