#!/usr/bin/python
# coding=utf-8

import sql_pool
import dao_util
import sql_template
from pymysql.cursors import DictCursor


def get_city():
    return dao_util.query_all(sql_pool.get_db(sql_pool.BASE_DATA_DB_NAME), sql_template.QUERY_ALL_CITY, raise_exp=True, cursorclass=DictCursor)


def get_suggested_source_city():
    return dao_util.query_all(sql_pool.get_db(sql_pool.SOURCE_INFO_DB_NAME), sql_template.QUERY_SUGGESTED_HOTEL_CITY)


if __name__ == '__main__':
    rows = get_suggested_source_city()
    for r in rows[:20]:
        print r
