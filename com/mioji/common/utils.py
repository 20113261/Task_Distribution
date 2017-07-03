#!/usr/bin/python
# coding=utf-8

import datetime

ONE_DAY_SE = 24*60*60


def iteration_append(old_iter, n_iter):
    """
    拼接两个迭代信息 数量 len(old_iter) * len(n_iter)
    :param old_iter: 
    :param n_iter: 
    :return: 
    """
    tmp = []
    if len(old_iter) == 0:
        return n_iter

    for o in old_iter:
        for a in n_iter:
            tmp.append(str(o)+':'+str(a))

    return tmp

if __name__ == '__main__':
    print get_task_date(datetime.date.today(), 7)
