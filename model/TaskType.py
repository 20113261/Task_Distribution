#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午6:50
# @Author  : Hou Rong
# @Site    : 
# @File    : TaskType.py
# @Software: PyCharm
import enum


class TaskType(enum.IntEnum):
    Flight = 0
    RoundFlight = 1
    MultiFlight = 2
    Hotel = 3
    Train = 4
    Bus = 5

    @staticmethod
    def parse_str(string):
        if isinstance(string, str):
            str_name = {
                'flight': TaskType.Flight,
                'roundflight': TaskType.RoundFlight,
                'multiflight': TaskType.MultiFlight,
                'hotel': TaskType.Hotel,
                'train': TaskType.Train,
                'bus': TaskType.Bus
            }

            _res = str_name.get(string.lower())
            if _res is None:
                raise TypeError("Unknown Enum: {}".format(string))
            else:
                return _res

        else:
            raise TypeError("Unknown Attr Type: {}".format(type(string)))


if __name__ == '__main__':
    print(TaskType.Flight)
    t = str(TaskType.Flight)
    print(TaskType(1))
    print(TaskType.parse_str("HOTEL"))
