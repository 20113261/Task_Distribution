#!/usr/bin/python
# coding=utf-8

from com.mioji.dao import baseinfo_dao
K_CITY_ID = 'id'
K_CITY_COUNTRY_ID = 'country_id'
K_DEGREE_COUNTRY_ID = 'trans_degree'
RANK_OTHER = "other"
RANK_CODE_DIC = {-1: RANK_OTHER, 0: "hub", 1: "portal"}
CHINA_CITY_ID = '101'


CITY_LIST = None
ID_CITY_DIC = {}
# 国家 : [city_id,]
COUNTRY_CITY_ID_DIC = {}
# 等级	hub－枢纽；portal－门户；other－普通城市; value city_id
RANK_CITY_ID_DIC = {'hub': [], 'portal': [], 'other': []}


def __init_info():
    rows = baseinfo_dao.get_city()
    global CITY_LIST
    CITY_LIST = rows
    for r in rows:
        city_id = r[K_CITY_ID]
        country_id = r[K_CITY_COUNTRY_ID]
        rank = r[K_DEGREE_COUNTRY_ID]
        ID_CITY_DIC[city_id] = r

        COUNTRY_CITY_ID_DIC.setdefault(country_id, [])
        COUNTRY_CITY_ID_DIC[country_id] += city_id
        RANK_CITY_ID_DIC[RANK_CODE_DIC.get(rank, RANK_OTHER)] += city_id


def get_city_list():
    return CITY_LIST


def get_city_info(city_id):
    return ID_CITY_DIC.get(city_id, None)

# 初始化
__init_info()

if __name__ == '__main__':
    print 'all city count', len(get_city_list())
    for k, v in RANK_CITY_ID_DIC.iteritems():
        print k, 'length', len(v)
    for c in get_city_list()[:10]:
        print c