#!/usr/bin/python
# coding=utf-8

import sql_pool
import dao_util
import sql_template
from com.mioji.common import crawl_type, task_class
from com.mioji.common.logger import logger
from memory_profiler import profile
from pymysql.cursors import DictCursor
import time


def insert_task_param(c_type, args):
    if crawl_type.T_HOTEL == c_type:
        sql = sql_template.INSERT_TASK_PARAM.format(type=c_type, columns='city_id,source_ids', values='%s,%s')
        dao_util.executemany(sql_pool.get_db(sql_pool.TASK_DB_NAME), sql, args)
    elif crawl_type.T_FLIGHT == c_type:
        pass


def insert_package_task_param(c_type, args):
    if crawl_type.T_HOTEL == c_type:
        sql = sql_template.INSERT_PACKAGE_TASK_PARAM.format(type=c_type)
        dao_util.executemany(sql_pool.get_db(sql_pool.TASK_DB_NAME), sql, args)
    elif crawl_type.T_FLIGHT == c_type:
        pass


def insert_crawl_task(c_type, args):
    useredis = False
    if useredis:
        import redis_util
        redis_util.insert_crawl_task(c_type, args)
    else:
        if crawl_type.T_HOTEL == c_type:
            sql = sql_template.INSERT_CRAWL_TASK_PARAM.format(type=c_type)
            dao_util.executemany(sql_pool.get_db(sql_pool.TASK_DB_NAME), sql, args)
        elif crawl_type.T_FLIGHT == c_type:
            pass


def get_task_param_model(c_type):
    rows = get_task_param(c_type)
    task_param_list = []
    for r in rows:
        t = task_class.HotelTaskParam(r)
        task_param_list.append(t)
    return task_param_list


def get_task_param(c_type, res_dict=False):
    query = 'select * from task_param_{type}'.format(type=c_type)
    return dao_util.query_all(sql_pool.get_db(sql_pool.TASK_DB_NAME), query,
                              cursorclass=DictCursor if res_dict else None)


@profile
def get_crawl_task_model(c_type):
    logger.debug('>> get_crawl_task')
    rows = get_crawl_task(c_type)
    logger.debug('<< get_crawl_task')
    task_param_list = []
    for r in rows:
        t = task_class.Task(r)
        task_param_list.append(t)
    del rows
    return task_param_list


def get_crawl_task(c_type, res_dict=False):
    query = 'select * from crawl_task_{type} limit 400000'.format(type=c_type)
    return dao_util.query_all(sql_pool.get_db(sql_pool.TASK_DB_NAME), query,
                              cursorclass=DictCursor if res_dict else None)


def get_package_task_relation(c_type, res_dict=False):
    query = 'select * from package_task_param_{type}'.format(type=c_type)
    return dao_util.query_all(sql_pool.get_db(sql_pool.TASK_DB_NAME), query,
                              cursorclass=DictCursor if res_dict else None)


if __name__ == '__main__':
    pass
