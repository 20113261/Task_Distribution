#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:03
# @Author  : Hou Rong
# @Site    : 
# @File    : config.py
# @Software: PyCharm


# TaskServer
server_port = 8087

# TaskDB
mongo_host = 'mongodb://root:miaoji1109-=@10.19.2.103:27017/'
mongo_base_task_db = 'RoutineBaseTask'
mongo_date_task_db = 'RoutineDateTask'

test_mongo_date_task_db = 'Test_RoutineDateTask'
# hotel_base_task_db = 'HotelRoutineBaseTask'

# TaskCollection
package_info_collection = 'package_info'

per_retrieve_count = 200

used_times_config = 2 #包括第一次

package_list = {
                'Flight': [-1, 0, 1, 2, 3, 4],
                'RoundFlight': [12, 13, 14],
                'MultiFlight': [15],
                'Hotel': [-1, 5, 6, 7, 8],
                'Train': [10],
                'Ferries': [60]
            }

frequency = {'-1': 1, '0': 1, '1': 2, '2': 4, '3': 8, '4': 10, '12': 1, '13': 4, '14': 7, '15': 3, '5': 1, '6': 2, '7': 8, '8': 8, '10':50, '60':10}

