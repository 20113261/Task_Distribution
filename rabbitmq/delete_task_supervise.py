#coding:utf-8
import pymongo
import datetime
import init_path
from conf import config
from collections import defaultdict
from mysql_execute import update_monitor
from logger_file import get_logger
from rabbitmq.supervise import get_source_info, update_dead_running, query_mongo
from logger_file import get_logger

logger = get_logger('delete_task_supervise')

client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]
base_task_db = client[config.mongo_base_task_db]

flight_package_list = [12, 13, 14]
hotel_package_list = [5, 6, 7, 8]

def delete_datetask_documents():
    '''
    每天零点定时执行一次！
    刪除已发完任务的文档,不能在监控中使用，因为会把失败的任务删掉从而被绿皮过滤掉。
    :return:
    '''
    delete_documents = defaultdict(dict)
    # 插入分源、分package_id，所有完成反馈任务的切片最大值信息
    for package_id, source, source_info in get_source_info():
        if source_info['record_count'] == source_info['fail_count'] + source_info['success_count']:
            if source_info['slice_num'] >= delete_documents.get(source, {}).get(package_id, {}).get('slice_num', 0):
                delete_documents[source][package_id] = source_info
    #删除小于当前完成反馈切片的mongo文档数据
    for delete_source, delete_info in delete_documents.items():
        for collection_name in date_task_db.collection_names():
            if delete_source in collection_name:
                for package_id, source_info in delete_info.items():
                    date_task_db[collection_name].remove({'slice_num': {'$lte': source_info['slice_num']}, 'package_id': package_id})
    logger.info(delete_documents)

if __name__ == '__main__':
    logger.info('开始删除已发完的任务：')
    query_mongo(hotel_package_list)
    delete_datetask_documents()
    logger.info('完成删除！')