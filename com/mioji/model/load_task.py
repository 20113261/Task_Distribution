#!/usr/bin/python
# coding=utf-8

from com.mioji.common import task_class, crawl_type
from com.mioji.dao import task_dao
from com.mioji.common.logger import logger
from memory_profiler import profile


def init_server():
    load_task()

# @profile
def load_task():
    c_type = crawl_type.T_HOTEL
    logger.debug('get_package_task_relation')
    relation = task_dao.get_package_task_relation(c_type)
    logger.debug('get_task_param_model')
    task_params = task_dao.get_task_param_model(c_type)
    logger.debug('get_crawl_task_model')
    # crawl_tasks = task_dao.get_crawl_task_model(c_type)
    logger.debug('set_init_model')
    task_class.set_init_task_model(c_type, task_params, relation)


if __name__ == '__main__':
    load_task()

