#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午4:47
# @Author  : Hou Rong
# @Site    : 
# @File    : mysql_execute.py
# @Software: PyCharm
import pymysql.cursors
from logger import get_logger

logger = get_logger("mysql_executor")


def fetchall(conn_pool, sql):
    conn = conn_pool.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    for line in cursor.fetchall():
        yield line
    cursor.close()
    conn.close()


def fetchall_ss(conn_pool, sql, size=1000):
    conn = conn_pool.connection()
    fetch_times = 9
    try:
        cursor = conn.cursor(cursor=pymysql.cursors.SSCursor)
        cursor.execute(sql)
        rows = cursor.fetchmany(size=size)
        while fetch_times:
            yield from rows
            logger.info('before fetchmany')
            rows = cursor.fetchmany(size)
            logger.info('after fetchmany')
            fetch_times -= 1
        cursor.close()
    except Exception as exc:
        logger.exception(msg="[sql error]", exc_info=exc)
    finally:
        logger.debug('finally')
        if conn:
            conn.close()


if __name__ == '__main__':
    from conn_pool import source_info_pool

    __sql = '''SELECT
  city.id AS city_id,
  city.trans_degree,
  city.grade,
  ota_location.source,
  ota_location.suggest,
  ota_location.suggest_type
FROM ota_location
  LEFT JOIN city ON ota_location.city_id
WHERE ota_location.city_id = 'NULL' AND source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') LIMIT 100;'''
    for line in fetchall_ss(conn_pool=source_info_pool, sql=__sql, size=10):
        pass

# fetchall_ss(conn_pool=source_info_pool, sql=__sql, size=100)
