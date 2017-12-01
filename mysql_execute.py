#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/1 下午4:47
# @Author  : Hou Rong
# @Site    : 
# @File    : mysql_execute.py
# @Software: PyCharm


def fetchall(conn_pool, sql):
    conn = conn_pool.connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    for line in cursor.fetchall():
        yield line
    cursor.close()
    conn.close()
