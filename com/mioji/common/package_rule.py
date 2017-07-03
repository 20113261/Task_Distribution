#!/usr/bin/python
# coding=utf-8

import task_class
from logger import logger


def hotel_package(city_info):
    """
    Rank0：
    城市：trans_degree=0/1的城市和grade=1/2
    更新频率：90天内1天一次
    Rank1：
    城市：grade=3/4的城市
    更新频率：90天内2天一次
    Rank2:
    城市：grade>=5的城市
    更新频率：90天内8天一次
    :param city_info: 
    :return: 
    """
    trans_degree = city_info.get('trans_degree', -1)
    grade = city_info.get('grade', -1)
    if trans_degree in [1, 2] or grade in [1, 2]:
        return [task_class.get_package(1)]
    elif grade in [3, 4]:
        return [task_class.get_package(2)]
    elif grade >= 5:
        return [task_class.get_package(3)]
    else:
        logger.warn('city: {0} trans_degree {1} grade {2} not in rules'.format(city_info['id'], trans_degree, grade))
        return []