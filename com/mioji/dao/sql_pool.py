#!/usr/bin/python
# coding=utf-8

import pymysql
from DBUtils.PooledDB import PooledDB
from com.conf import conf

TASK_DB_NAME = 'task_db'
BASE_DATA_DB_NAME = 'base_data'
SOURCE_INFO_DB_NAME = 'source_info'


def INIT_SQLPOOL(host='10.10.154.38', user='writer', passwd='miaoji1109', db='crawl', maxconnections=20):
    mysql_db_pool = PooledDB(creator=pymysql, mincached=1, maxcached=2, maxconnections=maxconnections,
                             host=host, port=3306, user=user, passwd=passwd,
                             db=db, charset='utf8', use_unicode=False, blocking=True)
    return mysql_db_pool


task_db = INIT_SQLPOOL(**conf.task_mysql)
base_data_db = INIT_SQLPOOL(**conf.basedata_mysql)
source_info_db = INIT_SQLPOOL(**conf.source_info)


def get_db(name, shareable=True):
    """
    :param name:  string  task_db、base_data、source_info
    :param shareable: bool
    :return: 
    """
    if TASK_DB_NAME == name:
        return task_db.connection(shareable=shareable)
    elif BASE_DATA_DB_NAME == name:
        return base_data_db.connection(shareable=shareable)
    elif SOURCE_INFO_DB_NAME == name:
        return source_info_db.connection(shareable=shareable)
    else:
        raise Exception('not found db:{0}'.format(db))


if __name__ == '__main__':
    db = get_db('task_db')
    cursor = db.cursor()
    cursor.execute('select * from task_param_hotel WHERE JSON_CONTAINS(source_ids,"2")')
    r = cursor.fetchall()
    cursor.close()
    db.close()

    print r
