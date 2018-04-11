import pymongo
import datetime
import init_path
from conf import config
import math
from model.TaskType import TaskType
from common.InsertDateTask import InsertDateTask
from common.InsertBaseTask import InsertBaseTask
from mysql_execute import fetchall, fetchall_ss, fetch_count, update_monitor
from conn_pool import base_data_pool, task_db_monitor_db_pool
from common.sql_query import supervise_supplement_city_sql
from logger_file import get_logger

logger = get_logger('supervise_supplement_city')

client = pymongo.MongoClient(host=config.mongo_host)
base_task_db = client[config.mongo_base_task_db]

def recreate_base_data(base_name):
    base_task_db[base_name].drop()
    insert_task = InsertBaseTask(task_type=TaskType.Flight)
    insert_task.insert_task()

def supplement_city_count():
    logger.info('开始监控本日城市数量：')
    flight_2_count = fetch_count(base_data_pool, supervise_supplement_city_sql['flight_package_id_2'])
    flight_3_count = fetch_count(base_data_pool, supervise_supplement_city_sql['flight_package_id_3'])
    flight_4_count = fetch_count(base_data_pool, supervise_supplement_city_sql['flight_package_id_4'])

    logger.info('flight_2_count:' + str(flight_2_count) + ', flight_3_count:' + str(flight_3_count) + ', flight_4_count:' + str(flight_4_count))

    flight_2_count_monitor = fetch_count(task_db_monitor_db_pool, supervise_supplement_city_sql['flight_count_monitor'].format(2))
    flight_3_count_monitor = fetch_count(task_db_monitor_db_pool, supervise_supplement_city_sql['flight_count_monitor'].format(3))
    flight_4_count_monitor = fetch_count(task_db_monitor_db_pool, supervise_supplement_city_sql['flight_count_monitor'].format(4))

    logger.info(
        'flight_2_count_monitor:' + str(flight_2_count_monitor) + ', flight_3_count_monitor:' + str(flight_3_count_monitor) + ', flight_4_count_monitor:' + str(
            flight_4_count_monitor))


    if flight_2_count > flight_2_count_monitor or flight_3_count > flight_3_count_monitor or flight_4_count > flight_4_count_monitor:
        logger.info('start to generate base data!')
        recreate_base_data('BaseTask_Flight')
        date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        update_monitor(task_db_monitor_db_pool, sql_list=[supervise_supplement_city_sql['update_flight_count'].format(i[0], i[1], date) for i in
                    [(2, flight_2_count), (3, flight_3_count), (4, flight_4_count)]])
        logger.info('finished incresing data')
    else:
        logger.info('本日无新增城市航线！')

if __name__ == '__main__':
    supplement_city_count()
