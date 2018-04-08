#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午6:50
# @Author  : Hou Rong
# @Site    : 
# @File    : TaskType.py
# @Software: PyCharm
import enum
from conf.config import package_list
from conf import task_source

class TaskType(enum.IntEnum):
    Flight = 0
    RoundFlight = 1
    MultiFlight = 2
    Hotel = 3
    Train = 4
    Bus = 5
    TempFlight = 6
    TempHotel = 7
    TempTrain = 8
    Ferries = 9

    @staticmethod
    def parse_str(string):
        if isinstance(string, str):
            str_name = {
                'flight': TaskType.Flight,
                'roundflight': TaskType.RoundFlight,
                'multiflight': TaskType.MultiFlight,
                'hotel': TaskType.Hotel,
                'train': TaskType.Train,
                'bus': TaskType.Bus,
                'tempflight': TaskType.TempFlight,
                'temphotel': TaskType.TempHotel,
                'ferries': TaskType.Ferries
            }

            _res = str_name.get(string.lower())
            if _res is None:
                raise TypeError("Unknown Enum: {}".format(string))
            else:
                return _res

        else:
            raise TypeError("Unknown Attr Type: {}".format(type(string)))


    @staticmethod
    def get_package_list(string):
        if isinstance(string, str):
            _res = package_list.get(string)
            if _res is None:
                raise TypeError("Unknown Attr Type: {}".format(type(string)))
            else:
                return _res
        else:
            raise TypeError("Unknown Attr Type: {}".format(type(string)))

    @staticmethod
    def get_source_list(string):
        source_list = {
            'Hotel': task_source.hotel_source,
            'TempHotel': task_source.hotel_source,
            'Flight': task_source.flight_source,
            'TempFlight': task_source.flight_source,
            'RoundFlight': task_source.round_flight_source,
            'MultiFlight': task_source.multi_flight_source,
            'Train': task_source.train_source,
            'Ferries': task_source.ferries_source
        }
        if isinstance(string, str):
            _res = source_list.get(string)
            if _res is None:
                raise TypeError("Unknown Attr Type: {}".format(type(string)))
            else:
                return _res
        else:
            raise TypeError("Unknown Attr Type: {}".format(type(string)))



if __name__ == '__main__':
    print(TaskType.Flight)
    t = str(TaskType.Flight)
    print(TaskType(1))
    print(TaskType.parse_str("HOTEL"))
    print(type(TaskType.parse_str("HOTEL")))

    s = TaskType.get_source_list('Train')
    print(s)