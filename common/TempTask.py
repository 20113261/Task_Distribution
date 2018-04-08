from model.TaskType import TaskType
from rabbitmq.consumer import query_temporary_task


class TempTask():
    def drop_collection(self, date_task_db, today):
        # 删除当天临时任务的重复生成。
        for collection_name in date_task_db.collection_names():
            if 'TemplateTask' in collection_name.split('_')[0]:
                if collection_name.split('_')[-1] == today:
                    date_task_db[collection_name].drop()

    @staticmethod
    def delete_single_slice(package_info, date_task_db, task_type):
        from common.InsertDateTask import InsertDateTask
        Temp_list = package_info.get_package()[task_type]
        package_id_list = []
        for package_info in Temp_list:
            if package_info.update_cycle == 24:
                package_id_list.append(package_info.package_id)
        for collection_name in date_task_db.collection_names():
            if 'TemplateTask' in collection_name.split('_')[0]:
                if collection_name.split('_')[-1] == InsertDateTask.today():
                    for package_id in package_id_list:
                        date_task_db[collection_name].remove({'package_id': package_id})

    @staticmethod
    def insert_task():
        from common.InsertDateTask import InsertDateTask
        query_list = query_temporary_task()
        for task_type, number in query_list:
            # if task_type == 'TempHotel':
            insert_date_task = InsertDateTask(TaskType.parse_str(task_type), number, True)
            insert_date_task.insert_task()
