#!/usr/bin/python
# coding=utf-8

import json
from com.mioji.dao import task_dao, baseinfo_dao
from com.mioji.common import city_info, base_value, crawl_type, package_rule


# utils
def create_task_param():
    rows = baseinfo_dao.get_suggested_source_city()
    suggested = {}
    for r in rows:
        source_id = base_value.SOURCE_S_ID.get(r[1], None)
        if source_id:
            suggested.setdefault(r[0], set([]))
            suggested[r[0]].add(source_id)

    city_list = city_info.get_city_list()
    task_param_args = []
    for city in city_list:
        city_id = city['id']
        s_list = sorted(list(suggested.get(city_id, [])))
        task_param_args.append((city_id, json.dumps(s_list)))

    task_dao.insert_task_param(crawl_type.T_HOTEL, task_param_args)


def create_all_tasks():
    """
    按规则package（package_rule） 生成 task_param/crawl_task/package_task relation 
    :return: 
    """
    task_param = task_dao.get_task_param(crawl_type.T_HOTEL)
    for task_info in task_param:
        city = city_info.get_city_info(task_info[1])
        pkg_t_args = []
        crawl_task_args = []
        for package in package_rule.hotel_package(city):
            source = json.loads(task_info[2])
            its = package.iteration_values
            pkg_t_args.append((package.id, task_info[0]))

            for s, i in __all_task(source, its):
                # (task_param_id, iteration_info, source_id)
                crawl_task_args.append((task_info[0], i, s))

        task_dao.insert_package_task_param(crawl_type.T_HOTEL, pkg_t_args)
        task_dao.insert_crawl_task(crawl_type.T_HOTEL, crawl_task_args)


def __all_task(source_ids, its):
    for s in source_ids:
        for i in its:
            yield s, i



