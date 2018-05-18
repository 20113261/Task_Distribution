import math
import datetime
from model.TaskType import TaskType
from common.InsertDateTask import InsertDateTask
from common.InsertBaseTask import InsertBaseTask

if __name__ == '__main__':
    # add_city_id = '''['60677']'''
    # insert_task = InsertBaseTask(task_type=TaskType.TempHotel, temporary_city_str=add_city_id, number=10006)
    # insert_task.insert_task()

    insert_task = InsertBaseTask(task_type=TaskType.Ferries)
    insert_task.insert_task()