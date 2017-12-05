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
    train = 4
    bus = 5

    @staticmethod
    def parse_str(string):
        if isinstance(string, str):
            str_name = {
                'flight': TaskType.flight,
                'roundflight': TaskType.round_flight,
                'multiflight': TaskType.multi_flight,
                'hotel': TaskType.hotel,
                'train': TaskType.train,
                'bus': TaskType.bus
            }

            _res = str_name.get(string.lower())
            if _res is None:
                raise TypeError("Unknown Enum: {}".format(string))
            else:
                return _res

        else:
            raise TypeError("Unknown Attr Type: {}".format(type(string)))


if __name__ == '__main__':
    print(TaskType.flight)
    print(TaskType(1))
    print(TaskType.parse_str("HOTEL"))
