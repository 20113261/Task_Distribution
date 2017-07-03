#!/usr/bin/python
# coding=utf-8

import dao_util
import sql_pool
import sql_template
from com.mioji.common import crawl_type, base_value


def create_tables_if_not_exist():
    """
    初始化数据库
    :return: 
    """
    for sql in sql_template.BASE_TABLE:
        dao_util.execute(sql_pool.get_db('task_db'), sql, raise_exp=True)

    for sql in sql_template.TYPES_TABLE:
        for c_type in crawl_type.CRAWL_TYPES:
            query = sql.format(type=c_type)
            dao_util.execute(sql_pool.get_db('task_db'), query, raise_exp=True)


def init_package_info():
    """
    初始化package
    :return: 
    """
    return dao_util.executemany(sql_pool.get_db(sql_pool.TASK_DB_NAME), sql_template.INSERT_PKG, base_value.package_list)







