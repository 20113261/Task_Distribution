import pymongo
from conf import config

client = pymongo.MongoClient(host=config.mongo_host)
# coll_1 = client['RoutineDateTask']['DateTask_Hotel_hotelsListHotel_20180513']
# coll_2 = client['RoutineDateTask']['DateTask_Hotel_hotelsListHotel_20180511']
#
# cursor_1 = coll_1.distinct('task_args.city_id')
# cursor_2 = coll_2.distinct('task_args.city_id')
# city_1 = []
# city_2 = []
# city = set()
# for i in cursor_1:
#     city_1.append(i)
#
# for i in cursor_2:
#     city_2.append(i)
#
# for i in city_1:
#     if i not in city_2:
#         city.add(i)
#
# print()

coll = client['RoutineDateTask']['DateTask_Flight_expediaFlight_20180516']
r = coll.create_index([ ('used_times', 1)])

res = set()
coll = client['case_result']['20180515']
cursor = coll.find({'source':{'$regex':'ListHotel'},'error':29, 'collection_name':{'$regex':'0515'}})
for i in cursor:
    res.add(i['tid'])
    # if i['content'] not in res:
    #     res.append(i['content'])
res2 = set()
coll2 = client['RoutineDateTask']['DateTask_Hotel_agodaListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'agodaListHotel'},'error_code':29}).hint([('source', 1), ('error_code', 1)])
for i in cursor2:
    res2.add(i['tid'])
coll2 = client['RoutineDateTask']['DateTask_Hotel_hotelsListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'hotelsListHotel'},'error_code':29})
for i in cursor2:
    res2.add(i['tid'])
coll2 = client['RoutineDateTask']['DateTask_Hotel_elongListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'elongListHotel'},'error_code':29})
for i in cursor2:
    res2.add(i['tid'])
coll2 = client['RoutineDateTask']['DateTask_Hotel_ctripListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'ctripListHotel'},'error_code':29})
for i in cursor2:
    res2.add(i['tid'])
coll2 = client['RoutineDateTask']['DateTask_Hotel_expediaListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'expediaListHotel'},'error_code':29})
for i in cursor2:
    res2.add(i['tid'])
coll2 = client['RoutineDateTask']['DateTask_Hotel_bookingListHotel_20180515']
cursor2 = coll2.find({'source':{'$regex':'bookingListHotel'},'error_code':29})
for i in cursor2:
    res2.add(i['tid'])




res3 = set()
for i in res:
    # content = i.split('&')[0]
    # date = i.split('&')[-1]
    # content = content + '_2_agodaListHotel_1_' + date
    if i not in res2:
        res3.add(i)
print()