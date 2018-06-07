from rabbitmq.supervise import query_mongo, update_package_info_collection, update_task_list_in_mysql, update_error_code_counter, get_average_success_count
from model.TaskType import TaskType

if __name__ == '__main__':

    slices_result, package_count_list = query_mongo('Hotel')
    update_package_info_collection(package_count_list)
    update_task_list_in_mysql('Hotel')

    update_error_code_counter(task_type=TaskType.Hotel)
