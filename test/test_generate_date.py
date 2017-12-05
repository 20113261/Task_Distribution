#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午10:35
# @Author  : Hou Rong
# @Site    : 
# @File    : test_generate_date.py
# @Software: PyCharm
import math
import datetime

if __name__ == '__main__':
    tasks_n = 10000
    days = (datetime.datetime.today() - datetime.datetime(2017, 1, 1)).days
    task_offset = days % 3

    '''
    math.ceil(float(10000) / float(3))
    3334
    
    [0, 3334)
    [3334, 6668)
    [6668, 10002)
    
    '''
    print(days, task_offset)
