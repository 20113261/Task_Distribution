#coding:utf-8
# @Time    : 2018/5/20
# @Author  : xiaopeng
# @Site    : boxueshuyuan
# @File    : _test_delete_task_supervise.py
# @Software: PyCharm

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
from model.TaskType import TaskType
from rabbitmq.pika_send import date_task_db
from rabbitmq.delete_task_supervise import delete_datetask_documents,drop_collection


# def delete_datetask_documents(task_type):
#     '''
#     每天零点定时执行一次！
#     刪除已发完任务的文档,不能在监控中使用，因为会把失败的任务删掉从而被绿皮过滤掉。
#     :return:
#     '''
#     delete_documents = defaultdict(dict)
#     # 插入分源、分package_id，所有完成反馈任务的切片最大值信息
#     for package_id, source, source_info in get_source_info(task_type):
#         if source_info['record_count'] == source_info['fail_count'] + source_info['success_count']:
#             if source_info['slice_num'] >= delete_documents.get(source, {}).get(package_id, {}).get('slice_num', 0):
#                 delete_documents[source][package_id] = source_info
#
# print()


if __name__ == '__main__':
    query_mongo('Flight')
    delete_datetask_documents('Flight')
    query_mongo('RoundFlight')
    delete_datetask_documents('RoundFlight')
    query_mongo('MultiFlight')
    delete_datetask_documents('MultiFlight')
    query_mongo('Train')
    delete_datetask_documents('Train')
    drop_collection()
    query_mongo('Ferries')
    delete_datetask_documents('Ferries')
    drop_collection()