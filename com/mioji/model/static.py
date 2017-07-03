#!/usr/bin/python
# coding=utf-8

from com.mioji.common import base_value, crawl_type, task_class

package_static = {}
source_static = {}
five_min_cache = {}


def init_static():
    ps = [task_class.get_package(i) for i in [1, 2, 3]]
    for p in ps:
        package_static[str(p.id)] = {
            'all': p.task_length,
            'oneday': p.oneday_task_length,
            'assigned': 0, 'retry': 0, 'feedback': 0,
            'success': 0, 'error': {}
        }

    # todo 各种统计
    for c_type in crawl_type.CRAWL_TYPES:
        source_static[c_type] = {}
        for _id, name in base_value.SOURCE_ID_S.iteritems():
            source_static[c_type][str(_id)] = {
                'all': 0,
                'oneday': 0,
                'assigned': 0, 'retry': 0, 'feedback': 0,
                'success': 0, 'error': {}
            }


def assign_tasklist(task_list, isretry=False):
    for task in task_list:
        assign(task, isretry=isretry)


def assign(task, isretry=False):
    def _append(info):
        info['assigned'] += 1
        if isretry:
            info['retry'] += 1

    # todo 各种统计
    ids = task[task_class.Task.TAG_ID]
    _append(package_static[str(ids[0])])
    _append(source_static[task[task_class.Task.TAG_CRYPE]][str(ids[2])])


def feedback(task):
    def _append(static):
        static['feedback'] += 1
        code = task['error']
        if code == 0:
            static['success'] += 1
        else:
            static['error'].setdefault(code, 0)
            static['error'][code] += 1

    # todo 各种统计
    ids = task[task_class.Task.TAG_ID]
    _append(package_static[str(ids[0])])
    _append(source_static[task[task_class.Task.TAG_CRYPE]][str(ids[2])])


def init_by_cache(cache):
    global package_static, source_static, five_min_cache
    package_static = cache['pkg_static']
    source_static = cache['s_static']
    five_min_cache = cache['five_static']


def stat():
    return {
        'pkg_static': package_static,
        's_static': source_static,
        'five_static': five_min_cache
    }
