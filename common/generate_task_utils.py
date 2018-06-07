import datetime
from conf.task_source import hotel_source
from conf.config import multiply_times

def sort_date_task_cursor(cursor):
    res = []
    for source in hotel_source:
        for line in cursor:
            if source == line['task_args']['source']:
                res.append(line)
    return res


def decide_need_hotel_task(cursor, take_count, memory_count_by_source):
    if take_count >= len(cursor):
        return cursor[-1], take_count
    line = cursor[take_count]
    package_id = line['package_id']
    source = line['task_args']['source']
    memory_count_by_source[source] = memory_count_by_source.get(source, 0) + multiply_times[str(package_id)]
    take_count += 1
    if memory_count_by_source[source] < 300000:
        return line, take_count
    else:
        return decide_need_hotel_task(cursor,take_count, memory_count_by_source)


def today_date(is_test):
    if is_test:
        return '20180531'
    return datetime.datetime.today().strftime('%Y%m%d')


if __name__ == '__main__':
    cursor = [{'task_args':{'source':'bookingListHotel'}},{'task_args':{'source':'agodaListHotel'}},   {'task_args':{'source':'elongListHotel'}}, {'task_args':{'source':'hotelsListHotel'}},{'task_args':{'source':'expediaListHotel'}},{'task_args':{'source':'ctripListHotel'}}]
    res = sort_date_task_cursor(cursor)
    print()
