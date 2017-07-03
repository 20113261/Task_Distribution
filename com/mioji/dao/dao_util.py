#!/usr/bin/python
# coding=utf-8
from com.mioji.common.logger import logger


def close(close_able):
    # noinspection PyBroadException
    try:
        if close_able is not None:
            close_able.close()
    except:
        logger.warn('close error:{0}'.format(close_able))
        pass


def execute(db, query, arg=None, raise_exp=False):
    """
    :param db: 
    :param query: 
    :param arg: 
    :param raise_exp: 
    :return: Number of rows affected, if any. 
    """
    rowcount = 0
    try:
        cursor = db.cursor()
        rowcount = cursor.execute(query, arg)
        db.commit()
        close(cursor)
    except Exception, e:
        if raise_exp:
            raise e
        else:
            logger.warn('executemany error {0}'.format(e))
    finally:
        close(db)
    return rowcount


def executemany(db, query, args, raise_exp=False):
    """
    :param db: 
    :param query: 
    :param args: 
    :param raise_exp: 是否抛出异常
    :param cursorclass:
    :return: Number of rows affected, if any.
    """
    rowcount = 0
    try:
        cursor = db.cursor()
        rowcount = cursor.executemany(query, args)
        db.commit()
        close(cursor)
    except Exception, e:
        if raise_exp:
            raise e
        else:
            logger.warn('executemany error {0}'.format(e))
    finally:
        close(db)
    return rowcount


def query_all(db, query, arg=None, raise_exp=False, cursorclass=None):
    rows = []
    try:
        cursor = db.cursor(cursor=cursorclass)
        cursor.execute(query, arg)
        rows = cursor.fetchall()
        close(cursor)
    except Exception, e:
        if raise_exp:
            raise e
        else:
            logger.warn('executemany error {0}'.format(e))
    finally:
        close(db)
    return rows