import pymongo
from conf import config
from collections import defaultdict

client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]

frequency = {'12':1, '13': 4, '14':7}

package_list = list(range(12, 15))
slices_result = defaultdict(list)
package_count_list = {}

#slice_num要在datetask的collection里面，然后下面的查询要用到。

for package_id in package_list:
    for slice_num in range(frequency.get(str(package_id))):
        per_package_records_count = 0
        for collection_name in date_task_db.collection_names():
            record_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num}).count()
            if record_count == 0:
                continue
            feedback_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'used_times': {'$gte': 1}}).count()
            success_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'finished': 1}).count()
            fail_count = date_task_db[collection_name].find({'package_id': package_id, 'slice_num': slice_num, 'finished': 0, 'used_times': 2}).count()
            slices_result[package_id].append({collection_name.split('_')[3]: {'record_count':record_count, 'feedback_count':feedback_count,
                                                                              'success_count':success_count, 'fail_count':fail_count, 'slice_num':slice_num}})
            per_package_records_count += record_count
        package_count_list[package_id] = {slice_num: per_package_records_count}

print(slices_result)
print(package_count_list)
