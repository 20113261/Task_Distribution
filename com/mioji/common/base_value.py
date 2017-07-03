#!/usr/bin/python
# coding=utf-8
from com.mioji.common import crawl_type

package_list_hotel = [
    (1, '酒店rank0:[7,90]/天', '', 0, 24, crawl_type.T_HOTEL, '{"day": [7, 90], "occ": [2, 2]}'),
    (2, '酒店rank1:[7,90]/2天', '', 1, 2*24, crawl_type.T_HOTEL, '{"day": [7, 90], "occ": [2, 2]}'),
    (3, '酒店rank2:[7,90]/8天', '', 2, 8*24, crawl_type.T_HOTEL, '{"day": [7, 90], "occ": [2, 2]}'),
]
package_list_flight = []
package_list = package_list_hotel + package_list_flight

SOURCE_AGODA = 'agoda'
SOURCE_BOOKING = 'booking'
SOURCE_CTRIP = 'ctrip'
SOURCE_ELONG = 'elong'
SOURCE_EXPEDIA = 'expedia'
SOURCE_HOTELTRAVEL = 'hoteltravel'

SOURCE_ID_S = \
    {1: SOURCE_AGODA,
     2: SOURCE_BOOKING,
     3: SOURCE_CTRIP,
     4: SOURCE_ELONG,
     5: SOURCE_EXPEDIA,
     6: SOURCE_HOTELTRAVEL}

SOURCE_S_ID = \
    {SOURCE_AGODA: 1,
     SOURCE_BOOKING: 2,
     SOURCE_CTRIP: 3,
     SOURCE_ELONG: 4,
     SOURCE_EXPEDIA: 5,
     SOURCE_HOTELTRAVEL: 6}

# wait init
section = {}
section_to_crawl_type = {}


def __init():
    for c_type in crawl_type.CRAWL_TYPES:
        section[c_type] = {}
        for _id, name in SOURCE_ID_S.iteritems():
            s_name = name + crawl_type.CRAWL_TYPE_FIX[c_type]
            section[c_type][_id] = s_name
            section_to_crawl_type[s_name] = c_type

__init()


def find_section_name(c_type, source_id):
    return section.get(c_type, {}).get(source_id, None)


def find_crawl_type_by_section_name(s_name):
    return section_to_crawl_type.get(s_name, None)