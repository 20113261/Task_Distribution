#!/usr/bin/python
# coding=utf-8
import json
from com.mioji.dao import init


def init_table():
    # base
    init.create_tables_if_not_exist()
    init.init_package_info()

    init_task_param()


def init_task_param():
    import init_hotel
    init_hotel.create_task_param()
    init_hotel.create_all_tasks()


if __name__ == '__main__':
    init_table()
    #init_task_param()
    # import init_hotel
    # init_hotel.create_all_tasks()
