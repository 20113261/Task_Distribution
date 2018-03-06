import pymongo
import datetime
import init_path
from conf import config
from collections import defaultdict
from mysql_execute import update_monitor
from logger_file import get_logger


client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]
base_task_db = client[config.mongo_base_task_db]

frequency = {'12':1, '13': 4, '14':7, '5':1, '6':2, '7': 8, '8':8}

flight_package_list = [12, 13, 14]
hotel_package_list = [5, 6, 7, 8]
slices_result = defaultdict(list)
package_count_list = {}
update_time = datetime.datetime.now().strftime('%Y%m%d%H') + '00'
update_day = datetime.datetime.now().strftime('%Y%m%d')

#昨天的任务先发？没写？
def query_mongo(package_list):
    '''
    分package_id,分collection查询datetask各集合中的任务状态。
    :return:
    slices_result为以package_id为键，值为以collection为单位统计的任务概览的列表,eg: defaultdict(<class 'list'>, {12: [{'ebookersRoundFlight': {'record_count': 32425, 'fail_count': 0, 'feedback_count': 101, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 39053, 'fail_count': 0, 'feedback_count': 90, 'slice_num': 0, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 50155, 'fail_count': 0, 'feedback_count': 129, 'slice_num': 0, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 31490, 'fail_count': 0, 'feedback_count': 182, 'slice_num': 0, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 34946, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'cheapticketsRoundFlight': {'record_count': 14000, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'ebookersRoundFlight': {'record_count': 40102, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 29655, 'fail_count': 0, 'feedback_count': 88, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 38150, 'fail_count': 0, 'feedback_count': 89, 'slice_num': 0, 'success_count': 0}}], 13: [{'ebookersRoundFlight': {'record_count': 49997, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 44634, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 60031, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 50343, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 46018, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'cheapticketsRoundFlight': {'record_count': 49478, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 53630, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 59166, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'ebookersRoundFlight': {'record_count': 58820, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'ebookersRoundFlight': {'record_count': 58908, 'fail_count': 0, 'feedback_count': 58, 'slice_num': 2, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 52246, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 64183, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'cheapticketsRoundFlight': {'record_count': 49132, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 50516, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}], 14: [{'ebookersRoundFlight': {'record_count': 50343, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 46883, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 54841, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 56398, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 0, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 50862, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'cheapticketsRoundFlight': {'record_count': 58128, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 52938, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 44115, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'ebookersRoundFlight': {'record_count': 52419, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 1, 'success_count': 0}}, {'ebookersRoundFlight': {'record_count': 49478, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'cleartripRoundFlight': {'record_count': 48786, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'pricelineRoundFlight': {'record_count': 49478, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'cheapticketsRoundFlight': {'record_count': 52246, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}, {'orbitzRoundFlight': {'record_count': 58474, 'fail_count': 0, 'feedback_count': 0, 'slice_num': 2, 'success_count': 0}}]})
    package_count_list为以package_id为键，值为以切片和所属切片的任务数量组成的字典，eg: {12: {0: 309976}, 13: {0: 205005, 1: 267112, 2: 274985, 3: 0}, 14: {0: 208465, 1: 258462, 2: 258462, 3: 0, 4: 0, 5: 0, 6: 0}}
    '''
    #slice_num要在datetask的collection里面，然后下面的查询要用到。
    for package_id in package_list:
        # if package_id != 5:
        #     continue
        package_count_list[package_id] = {}
        slice_num_list = range(frequency.get(str(package_id))) if package_id != 100 else range(30)
        for slice_num in slice_num_list:
            per_package_records_count = 0
            for collection_name in date_task_db.collection_names():
                # if 'ctrip' not in collection_name:
                #     continue
                record_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num}).count()
                if record_count == 0:
                    continue
                feedback_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'used_times': {'$gte': 1}}).count()
                success_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'finished': 1}).count()
                fail_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'finished': 0, 'used_times': 2}).count()

                total_used_times = date_task_db[collection_name].aggregate([{'$match':{'package_id': package_id, 'slice_num': slice_num}}, {'$group':{'_id':'null','total_used_times': {'$sum':'$used_times'}}}, {'$project':{'_id':0, 'total_used_times':1}}])
                for used_times in total_used_times:
                    total_used_times = used_times['total_used_times'] - feedback_count
                total_take_times = date_task_db[collection_name].aggregate([{'$match': {'package_id': package_id, 'slice_num': slice_num}}, {'$group':{'_id':'null', 'total_take_times': {'$sum':'$take_times'}}}, {'$project':{'_id':0, 'total_take_times':1}}])
                for take_times in total_take_times:
                    total_take_times = take_times['total_take_times']
                slices_result[package_id].append({collection_name.split('_')[-2]: {'slice_num':slice_num, 'record_count':record_count, 'feedback_count':feedback_count,
                                'success_count':success_count, 'fail_count':fail_count, 'total_used_times': total_used_times, 'total_take_times':total_take_times}})

                per_package_records_count += record_count
            package_count_list[package_id][slice_num] = per_package_records_count

    print(slices_result)
    print(package_count_list) #bug:每次生成的总次数不一致。
    return slices_result, package_count_list

def delete_datetask_documents():
    '''
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
    print(delete_documents)


def update_package_info_collection(package_count_list):
    '''
    更新package_info里的next_slice字段，明示是否可再生成新切片任务。
    :param package_count_list:
    :return:
    '''
    for package_id, info in package_count_list.items():
        if len(info) == 0:
            base_task_db['package_info'].update({'id': package_id}, {'$set': {'next_slice': 1}}) #package_id在collection里的字段是id。
        else:
            do_next_slice = 0
            for slice_num, count in info.items():
                if count > 0:
                    do_next_slice += 1
            if do_next_slice <= 1:
                base_task_db['package_info'].update({'id': package_id}, {'$set': {'next_slice': 1}})
            else:
                base_task_db['package_info'].update({'id': package_id}, {'$set': {'next_slice': 0}})
            # if do_next_slice

# slices_result = {12: [{'ebookersRoundFlight': {'success_count': 0, 'record_count': 32524, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 31659, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 27853, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 29756, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 38233, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}], 13: [{'ebookersRoundFlight': {'success_count': 0, 'record_count': 49997, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 44634, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 60031, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 60031, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 50343, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 46018, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 49478, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 53630, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 59166, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'ebookersRoundFlight': {'success_count': 0, 'record_count': 58820, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'ebookersRoundFlight': {'success_count': 0, 'record_count': 58993, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 52246, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 64183, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 49132, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 50516, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}], 14: [{'ebookersRoundFlight': {'success_count': 0, 'record_count': 50343, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 46883, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 49997, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 54841, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 56398, 'slice_num': 0, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 50862, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 58128, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 52938, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 44115, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'ebookersRoundFlight': {'success_count': 0, 'record_count': 52419, 'slice_num': 1, 'feedback_count': 0, 'fail_count': 0}}, {'ebookersRoundFlight': {'success_count': 0, 'record_count': 49478, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'cleartripRoundFlight': {'success_count': 0, 'record_count': 48786, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'pricelineRoundFlight': {'success_count': 0, 'record_count': 49478, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'cheapticketsRoundFlight': {'success_count': 0, 'record_count': 52246, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}, {'orbitzRoundFlight': {'success_count': 0, 'record_count': 58474, 'slice_num': 2, 'feedback_count': 0, 'fail_count': 0}}]}


def update_task_list_in_mysql():
    '''
    更新与绿皮关联的mysql表
    :return:
    '''
    from conn_pool import task_db_monitor_db_pool
    conn_pool = task_db_monitor_db_pool
    sql_list = []
    for package_id, source, source_info in get_source_info():
        sql_insert = '''replace into task_day_list_monitor (source,package_id,slice_num,frequency,total_generated,retrieve_count,
                      feedback_count,success_count,fail_count,datetime,date,total_take_times,total_used_times)
                      VALUE ("%s",%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%d,%d);'''%(source,package_id,source_info['slice_num'],frequency.get(str(package_id)),
                      source_info['record_count'],0,source_info['feedback_count'],source_info['success_count'],
                        source_info['fail_count'],update_time,update_day,source_info['total_take_times'],source_info['total_used_times'])
        sql_list.append(sql_insert)
    update_monitor(conn_pool, sql_list, update_time)


def get_source_info():
    for package_id, package_content in slices_result.items():
        for source_content in package_content:
            for source, source_info in source_content.items():
                yield package_id, source, source_info #source_info:任务概览，eg:{'record_count': 32425, 'fail_count': 0, 'feedback_count': 101, 'slice_num': 0, 'success_count': 0}

def update_dead_running():
    supervise_time = datetime.datetime.now()+ datetime.timedelta(minutes=20)
    for collection_name in date_task_db.collection_names():
        date_task_db[collection_name].update({'run': 1, 'update_time': {'$lt': supervise_time}}, {'$set': {'run': 0}}, multi=True)

if __name__ == '__main__':
    update_dead_running()
    slices_result, package_count_list = query_mongo(hotel_package_list)

    # delete_datetask_documents() #不能把当前切片的数据删除掉，否则无数据

    update_package_info_collection(package_count_list)
    update_task_list_in_mysql()

    # supervise_time = datetime.datetime.now()+ datetime.timedelta(minutes=-1)
    # date_task_db['DateTask_Round_Flight_cleartripRoundFlight_20180234'].update({'run': 1, 'update_time': {'$lt': supervise_time}}, {'$set': {'run': 0}},
    #                                      multi=True)
    print('wanle')

