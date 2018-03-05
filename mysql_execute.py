#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午4:47
# @Author  : Hou Rong
# @Site    : 
# @File    : mysql_execute.py
# @Software: PyCharm
import pymysql.cursors
from logger_file import get_logger
logger = get_logger("mysql_executor")

def fetchall(conn_pool, sql):
    conn = conn_pool.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    for line in cursor.fetchall():
        yield line
    cursor.close()
    conn.close()


def fetchall_ss(conn_pool, sql, size=10000):
    conn = conn_pool.connection()
    fetch_times = 65
    try:
        cursor = conn.cursor(cursor=pymysql.cursors.SSCursor)
        cursor.execute(sql)
        # res = cursor.fetchall()
        count = 0
        # while fetch_times:
        #     res = cursor.fetchmany(size)
        #     count += len(res)
        #     print(count)
        #     fetch_times -= 1
        # rows = cursor.fetchmany(size=size)
        while fetch_times:
            logger.info('before fetchmany')
            rows = cursor.fetchmany(size)
            yield from rows
            count += len(rows)
            print('*********************', count)
            logger.info('after fetchmany')
            fetch_times -= 1
        if cursor:
            cursor.close()
    except Exception as exc:
        logger.exception(msg="[sql error]", exc_info=exc)
    finally:
        logger.debug('finally')
        if conn:
            conn.close()


def update_monitor(conn_pool, sql_list, update_time):
    try:
        # delete_sql = '''
        #     delete from task_day_list_monitor where datetime='%s';
        # ''' % update_time
        conn = conn_pool.connection()
        cursor = conn.cursor()
        print(dir(conn))
        # cursor.execute(delete_sql)
        for sql in sql_list:
            cursor.execute(sql)
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.exception(msg="[sql error]", exc_info=e)
        logger.info(sql_list)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    from conn_pool import source_info_pool

#     __sql = '''SELECT
#   city.id AS city_id,
#   city.trans_degree,
#   city.grade,
#   ota_location.source,
#   ota_location.suggest,
#   ota_location.suggest_type
# FROM ota_location
#   LEFT JOIN city ON ota_location.city_id
# WHERE ota_location.city_id = 'NULL' AND source IN ('booking', 'agoda', 'elong', 'hotels', 'expedia', 'ctrip') LIMIT 100;'''
#     for line in fetchall_ss(conn_pool=source_info_pool, sql=__sql, size=10):
#         pass
# fetchall_ss(conn_pool=source_info_pool, sql=__sql, size=100)


    from conn_pool import task_db_monitor_db_pool
    conn_pool = task_db_monitor_db_pool
    sql = '''
    insert into task_day_list_monitor (source,package_id,slice_num) VALUE ('p',3,2);
    '''

    update_monitor(conn_pool, sql)