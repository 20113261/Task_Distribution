from pika import adapters
import pika
import datetime
from rabbitmq import pika_send
from logger_file import get_logger
from mysql_execute import update_monitor, fetchall
from conn_pool import task_db_monitor_db_pool
from model.TaskType import TaskType

logger = get_logger('server')
logger2 = get_logger('consumer')
content = []


def insert_spider_result(result):
    '''
    爬虫入库，可以增加mongo数据入库出现异常的处理（暂未）
    :param result:
    :return:
    '''
    try:
        today = datetime.datetime.today().strftime('%Y%m%d')
        # for task_info in result:
        #     task_info['update_time'] = datetime.datetime.now()
        for i in range(len(result)):
            result[i].update({'update_time': datetime.datetime.now().strftime('%Y%m%d%H%M%S')})
        pika_send.client['case_result'][today].insert_many(result)
            # logger.info('tid:%s'%(task_info['tid']))
        if 'Hotel' in result[0]['source']:
            logger.info('完成此次酒店爬虫入库')
        elif 'Flight' in result[0]['source']:
            logger.info('完成此次飞机爬虫入库')
        else:
            logger.info('完成爬虫入库')
    except Exception as e:
        logger2.error("mongo发生异常", exc_info=1)

def feed_back_date_task(result):
    '''
    状态反馈，可以增加更新mongo数据出现异常的处理（暂未）
    :param result:
    :return:
    '''
    try:
        for task_info in result:
            used_times = task_info['used_times'] + 1
            feedback_times = task_info['feedback_times'] + 1
            if task_info['error'] == 0:
                pika_send.date_task_db[task_info['collection_name']].update({'tid': task_info['tid']}, {'$set': {'finished': 1,
                    'run': 0, 'used_times': used_times, 'feedback_times': feedback_times, 'error_code': task_info['error']}})
            else:
                pika_send.date_task_db[task_info['collection_name']].update({'tid': task_info['tid']}, {'$set': {'run': 0,
                    'used_times': used_times, 'feedback_times': feedback_times, 'error_code': task_info['error']}})
            # if task_info['error'] == 0:
            #     pika_send.date_task_db[task_info['collection_name']].update({'tid': task_info['tid']}, {'$set': {'finished': 1, 'run': 0, 'used_times': used_times}})
            # else:
            #     pika_send.date_task_db[task_info['collection_name']].update({'tid': task_info['tid']}, {'$set': {'run': 0, 'used_times': used_times}})

        logger.info('完成此次状态反馈')
    except Exception as e:
        logger2.error("mongo发生异常", exc_info=1)


def slave_take_times(response):
    try:
        for line in response:
            if isinstance(line, str):
                line = eval(line)
            take_times = line['take_times'] + 1
            pika_send.date_task_db[line['collection_name']].update({'tid': line['tid']}, {'$set': {'take_times': take_times}})
        logger.info('完成取走次数更新')
    except Exception as e:
        logger2.error("取走次数更新异常", exc_info=1)


def task_temporary_monitor(task_type, number):
    update_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    task_type = str(task_type).split('.')[-1]
    sql = '''insert into task_temporary_monitor(type,number,datetime) values('{}',{},'{}');'''.format(task_type, number, update_time)

    update_monitor(task_db_monitor_db_pool, sql_list=[sql])


def query_temporary_task():
    sql = '''select type,number from task_temporary_monitor'''
    query_list = []
    for row in fetchall(task_db_monitor_db_pool, sql):
        query_list.append((row[0], row[1]))
    return query_list

