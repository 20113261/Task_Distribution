#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:13
# @Author  : Hou Rong
# @Site    : 
# @File    : DbLoader.py
# @Software: PyCharm


class DbLoader(object):
    def __init__(self):
        self.base_mysql_info = {
            'host': '10.10.238.148',
            'user': 'mioji_admin',
            'password': 'mioji1109',
            'db': 'task_db',
            'charset': 'utf8'
        }
        self.env = 'test'

    def reload_base_table(self):
        pass

    def load_city_info(self):
        return {
            'cid': '',
            'tri_code': '',
            'grade': '',
            'trans_degree': ''
        }

    def load_xxx(self):
        '''
        if (t_flight == type)
        sql = "select dept,dest,source,package_id from task_flight";
    else if (t_hotel == type)
        sql = "select city_id,source,package_id from task_hotel";
    else if (t_train == type)
        sql = "select dept,dest,source,package_id from task_train";
    else if (t_bus == type)
        sql = "select dept,dest,source,package_id from task_bus";
	else if (t_roundflight == type)
		sql = "select dept,dest,source,package_id from task_roundflight";
	else if (t_multiflight == type)
		sql = "select dept,dest,source,package_id from task_multiflight";
        :return:
        '''
        pass

    def load_source_info(self):
        '''
        select source,upperbound from workload_valve where upperbound > 0
        source_frequency_map source upperbound
        '''

        pass

    def load_rule_info(self):
        pass

    def load_package_info(self):
        pass

    def all_task_info(self):
        pass
