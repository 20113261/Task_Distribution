import sys
import requests, json, random
import time
from bson.objectid import ObjectId
from bson import json_util
from logger_file import get_logger
import traceback
from collections import defaultdict
from common.generate_task_utils import sort_date_task_cursor
from conf.config import multiply_times

logger = get_logger('test')

# try:
#     a = 2/0
# except Exception as e:
#     logger.info(traceback.format_exc(e))

memory_count_by_source = {'ctripListHotel':0,
    'expediaListHotel':0,
    'agodaListHotel':0,
    'elongListHotel':5,
    'hotelsListHotel':2955,
    'bookingListHotel':8}


def decide_need_hotel_task(cursor, take_count):
    if take_count >= len(cursor):
        return cursor[-1], take_count
    line = cursor[take_count]
    package_id = line['package_id']
    source = line['task_args']['source']
    memory = memory_count_by_source.get(source, 0) + multiply_times[str(package_id)]
    take_count += 1
    if memory < 300000:
        return line, take_count
    else:
        return decide_need_hotel_task(cursor,take_count)


def source_take_test():
    res = []
    #{'task_args': {'source': 'ctripListHotel'}, 'package_id': 5},
    cursor = [ {'task_args':{'source':'expediaListHotel'}, 'package_id':5},
              {'task_args':{'source':'bookingListHotel'}, 'package_id':5},
              {'task_args': {'source': 'hotelsListHotel'}, 'package_id': 5}, {'task_args': {'source': 'elongListHotel'}, 'package_id': 5}]
    cursor = [
        {
            "_id": ObjectId("5ad5ee2380e566549e47ea66"),
            "task_type": 3,
            "tid": "1cf166a611e510c5ca2d33ea9d522cc5",
            "task_args": {
                "source": "agodaListHotel",
                "country_id": "null",
                "suggest": "[{\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF57htlOwABYNhHHf4lNKH9iYt%2fsKxy0E9PlwQYQamWmQmF3XtrBt6d1dpRsfWZGyyG39uYvwN5W2OKVrIy6qvXAL%2bvkLuDIQXrj58SPH0Oxq89e1N2YFi5xHTc8SsTcfdn4Vu6LfAjDILIgNfhBaz5LefwzMDXWmrk6G5AmPTrC7Mmh%2fnhLzA%2bVOz06ioT5dx970FCf%2fVjEZtBKglpIVFSuacY%2fwPspU3wBrNaiQo5vZ7weIFYLKHxyFPh0lWrMycw%3d%3d&city=19557&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"亚萨瓦群岛\", \"ObjectID\": 19557}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF72wEOf0En6N%2bOW6k4lvLFEsnJupY%2fSILTmoe4eyLsdrB11P9kkYiVaLs000IKqR8EZnDlk3%2b%2fbDAMCRwRBjxhdVp13YReR7AkNGtExnl2DTxnCtuX8I%2bMb88Vp6etYRNcHzbUHALU9W2KZtgMzZStOU5VH%2fr4EeDtt7eto51aR4ufwvKP6gRGtdQ3oRVdDqFRKSsmY61cvLzcmxDji3ChxXbBJlPDcezModExOhewiv3SFoHfX5GQQ3iut%2b4HZ6FA%3d%3d&city=669661&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"塞舌尔群岛\", \"ObjectID\": 669661}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF%2fgMg06yIPGyoDPiwOgBrQXWh%2bArpqZRyexiTg5ToZvOUtAeTOPZeLhfVhC8EPJKORqs6DsRjygdWqLXv93SKup0yGlO890SVVQKupbUtmtKdcA%2bV5E1P4rO9hQDhOYwVP%2bXktMzO8a9zHKoRmLZf%2fvnoLWhk52OrbahekydB7IaC7kjPtesi%2fxxAQqYINSaINa6y1PIeTMCLVvkGQ4k5QBtLd3PNpyf6ftBl5MCuYUPOITG990FS07SZNwZvB4z4g%3d%3d&city=17759&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"马尔代夫群岛\", \"ObjectID\": 17759}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOFwAw5O2SkpXpB2frNkulkfGmE0pNUfgTEAngtXSbJ7MDkcDrBVqv1aKIhvWYQBkJFO0ewg1xsraPIhgLroptl0lV6pdCzNC9QReolOLsRTs1mgkJDRWyXYs%2bhuEHG%2bLQIPjOaVAK4Ht9lzqApPhFwKoKjaqllNcOdLNPP2KhcLG%2fxX39ny2pDf2TMJwRXG8at%2bNXkMrN4L8GSQsLfM4C6EPs5t3YtLkleXGAktpRrekKVej0HbGYOsDI4qJ8gEonfA%3d%3d&area=31233&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \" 亚萨瓦岛\", \"ObjectID\": 31233}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOFyVUwGg7zyYF9gZ5Mb1kbxTD3jUCqoIl6Otu3SUQy2HnCtRIzj4JbSieu8s9sxTjdTb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaxJI8JYnZ1kv5yHlu1CSWFLO0BdvEA8oATMIgruGe7Xvgf8X1TSVstWcRZbk3B5woEY9g2a7A6ag1WYnfm7jxWQW%2fmefF6802aL64jNMttaaJAhtK0aya9Bb8XutvuFha%2fgvBnQ0XaWENBk1aLIqafg%3d%3d&area=34402&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \"吉利群岛\", \"ObjectID\": 34402}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF57htlOwABYNhHHf4lNKH9iJ1BmzOWdSHZCBukoHuG7S8aqJazj7hNOqVmizYUL%2biDb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaxJI8JYnZ1kv5yHlu1CSWFPP5SZ5OuUQfNLp9cp9NCojgf8X1TSVstWcRZbk3B5woEY9g2a7A6ag1WYnfm7jxWQW%2fmefF6802aL64jNMttaaHneQUXQmFPLI7eX2u93DJljbhP%2fUeEsISSmQnbHyr%2fA%3d%3d&area=517704&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \"亚萨马尔\", \"ObjectID\": 517704}]",
                "city_id": "30166",
                "suggest_type": 0,
                "ticket_info": {},
                "content": ""
            },
            "package_id": 5
        }
        ,
        {
            "_id": ObjectId("5ad5ee2380e566549e47ecf6"),
            "task_type": 3,
            "tid": "e3b2a9ac603eb9bf0dd4be0b7851a8bb",
            "task_args": {
                "source": "expediaListHotel",
                "country_id": "null",
                "suggest": "[{\"hierarchyInfo\": {\"country\": {\"name\": \"斐济\", \"isoCode2\": \"FJ\", \"isoCode3\": \"FJI\"}, \"airport\": {\"airportCode\": \"NAN\", \"multicity\": \"6023598\"}}, \"index\": \"0\", \"packageable\": \"false\", \"regionNames\": {\"fullName\": \"亚萨瓦群岛, 斐济\", \"lastSearchName\": \"亚萨瓦群岛, 斐济\", \"displayName\": \"<B>亚萨瓦群岛</B>, <B>斐济</B>\", \"shortName\": \"亚萨瓦群岛\"}, \"popularity\": \"7.0\", \"coordinates\": {\"lat\": \"-17.027000\", \"long\": \"177.143500\"}, \"hotelCountPerRegion\": \"15\", \"score\": \"1.0\", \"regionGrades\": {\"globalGrade\": \"0\"}, \"essId\": {\"sourceName\": \"GAI\", \"sourceId\": \"6049187\"}, \"gaiaId\": \"6049187\", \"type\": \"NEIGHBORHOOD\", \"@type\": \"gaiaRegionResult\", \"regionMetadata\": {\"hotelCount\": \"15\"}}]",
                "city_id": "30166",
                "suggest_type": 0,
                "ticket_info": {},
                "content": ""
            },
            "package_id": 5
        }
        ,
        {
            "_id": ObjectId("5ad5ee2480e566549e47f0d6"),
            "task_type": 3,
            "tid": "f9dfc389f773f5c2c7637f14fdbdefbf",
            "task_args": {
                "source": "elongListHotel",
                "country_id": "null",
                "suggest": "[{\"hotelCnt\": \"1\", \"composedName\": \"亚萨瓦岛-斐济\", \"stateNameCn\": \"斐济\", \"regionId\": \"10523\", \"regionNameEn\": \"Yasawa Island\", \"countryNameCn\": \"斐济\", \"regionNameCn\": \"亚萨瓦岛\"}]",
                "city_id": "30166",
                "suggest_type": 0,
                "ticket_info": {},
                "content": ""
            },
            "package_id": 5
        }
        ,
        {
            "_id": ObjectId("5ad5ee2480e566549e47f424"),
            "task_type": 3,
            "tid": "506576913115676a680893b7fa1fe04e",
            "task_args": {
                "source": "hotelsListHotel",
                "country_id": "null",
                "suggest": "[{\"name\": \"亚萨瓦岛\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.52312406, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span>, <span class='highlighted'>斐济</span>\", \"destinationId\": \"1638702\", \"latitude\": -16.74961406, \"landmarkCityDestinationId\": null, \"type\": \"CITY\", \"geoId\": \"1000000000000010523\"}, {\"name\": \"亚萨瓦岛度假村\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.53165, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span>度假村, 亚萨瓦岛, <span class='highlighted'>斐济</span>\", \"destinationId\": \"272960\", \"latitude\": -16.76407, \"landmarkCityDestinationId\": null, \"type\": \"HOTEL\", \"geoId\": \"1100000000000272960\"}, {\"name\": \"姆布卡马\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.554245, \"caption\": \"姆布卡马, <span class='highlighted'>斐济</span> (<span class='highlighted'>亚萨瓦岛</span>)\", \"destinationId\": \"1771406\", \"latitude\": -16.75961619, \"landmarkCityDestinationId\": null, \"type\": \"CITY\", \"geoId\": \"1000000000006269054\"}, {\"name\": \"亚萨瓦岛 (YAS)\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.545556, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span> (YAS), <span class='highlighted'>斐济</span>\", \"destinationId\": \"1779614\", \"latitude\": -16.758889, \"landmarkCityDestinationId\": null, \"type\": \"MINOR_AIRPORT\", \"geoId\": \"1000000000006285185\"}]",
                "city_id": "30166",
                "suggest_type": 0,
                "ticket_info": {},
                "content": ""
            },
            "package_id": 5
        }
    ]
    # random.shuffle(cursor)
    # cursor = [{'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'hotelsListHotel', 'ticket_info': {}, 'suggest_type': 0, 'suggest': '[{"name": "Yaounde", "m_url": "https://zh.hotels.com/search.do?destination-id=258832&q-check-in=2017-05-12&q-room-0-adults=2&q-destination=Yaounde%2C+Cameroon&resolved-location=CITY%3A258832%3AUNKNOWN%3AUNKNOWN&q-rooms=1&q-room-0-children=0&q-check-out=2017-05-13", "redirectPage": "DEFAULT_PAGE", "longitude": 11.53391106, "caption": "Yaounde, Cameroon", "destinationId": "258832", "latitude": 3.86740467, "landmarkCityDestinationId": null, "type": "CITY", "geoId": "1000000000000003886"}]'}, 'task_type': 3, 'tid': '8ae3ed5e278617dbbf03b5ba2a3a3c22', '_id': ObjectId('5ad5ee2180e566549e47d805'), 'need_breakfast_column': 2, 'package_id': 5}, {'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'elongListHotel', 'ticket_info': {}, 'suggest_type': 0, 'suggest': '[{"hotelCnt": "27", "composedName": "雅温德(Yaounde)-喀麦隆", "m_url": "http://ihotel.elong.com/region_3886", "stateNameCn": "喀麦隆", "regionId": "3886", "regionNameEn": "Yaounde", "countryNameCn": "喀麦隆", "regionNameCn": "雅温德"}]'}, 'task_type': 3, 'tid': '9e20b155b55671dacb3ac12e49552653', '_id': ObjectId('5ad5ee1e80e566549e47ba15'), 'need_breakfast_column': 0, 'package_id': 5}, {'package_id': 5, 'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'agodaListHotel', 'ticket_info': {}, 'suggest_type': 0, 'suggest': '[{"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI%2b1nmDSPGC623D3eHVDv6JlN2jcr0MizlS8Y%2bIrgxTFsIyRrmZNuO4JEaDx8eznEh5Eu26pkUFDpiPe7zeq1R6UiOGQjCRqtvugSOJfYh77LiDj0Wxxs%2f8f%2bJCQUj32AzfuqTLAZLSyJ9gCyxAL1xDkd%2bYbVu0nitcgaq4HocuVY1JsdwdpdBrVkYiJUSC5POk%3d&city=8102&tick=636235555645", "ObjectTypeID": 5, "SuggestionType": 2, "Name": "雅温得", "ObjectID": 8102}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF0Xja%2ftpnb803tBpmzl%2bZ8YYLq%2bw4DI9TfT%2bgYOfyyPPZaOoeY7Skwyjupb89Me28Z0Ol6GnRgUry8P8OiXKX92hZM3PA9aTXo4PX%2b3l5Cb5kI2OY%2bkVOzT7Yatmd02nylQNjq3Y8Hr0I1wSjn%2fQd%2fe4vVNRO8X9Mb3vL5TiNZd8Bp0eoREr2xLYHgqmk0Io4N1cb46s%2fzaPNE3EidG6PjURIP3As62OoJqmINhDbrmU&city=18231&tick=636235555646", "ObjectTypeID": 5, "SuggestionType": 2, "Name": "温得和克", "ObjectID": 18231}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF0ZXX3iKqUL7oERMGEKbR2T1T51Gk8ALTYKAXe97BzKIPrUSeK1W9iNzkxom%2f0QxzEzQM6Rkwzm4hKAiNXvJk1yMka5mTbjuCRGg8fHs5xIeRLtuqZFBQ6Yj3u83qtUelIjhkIwkarb7oEjiX2Ie%2by4g49FscbP%2fH%2fiQkFI99gM37qkywGS0sifYAssQC9cQ5HfmG1btJ4rXIGquB6HLlWNii6c9DYfE%2fijR5ue6ZNle&poi=59720&tick=636235555646", "ObjectTypeID": 12, "SuggestionType": 2, "Name": "喀麦隆雅温得机场", "ObjectID": 59720}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI%2b1nmDSPGC623D3eHVDv6JlPue4UQ83VP2nWySCiPMupIyRrmZNuO4JEaDx8eznEh5Eu26pkUFDpiPe7zeq1R6UiOGQjCRqtvugSOJfYh77LiDj0Wxxs%2f8f%2bJCQUj32AzfuqTLAZLSyJ9gCyxAL1xDkd%2bYbVu0nitcgaq4HocuVYx19YSIcovR%2f0ZnvDzYvit4%3d&area=23151&tick=636235555646", "ObjectTypeID": 6, "SuggestionType": 2, "Name": "雅温得", "ObjectID": 23151}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF4EVcqAQ5AwwZdxhINUOcBQjhD37ys%2fFU6lN7cuMgW2ijI84yWSnLTvNRDJA%2fhRDwTb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaIW6M7bKripp4Z%2fN%2f1mPbaPHgqJuKO9FTQewa9IJFCFdJbjP94v%2fAEU%2bFMV2YvDqAbbYhAtfVY%2bSwpUg5x%2fQA4cKA4NNzHJcSJJKF2WAjXF444SB8L40JMO9PYZz%2fFtP2&area=534022&tick=636235555646", "ObjectTypeID": 6, "SuggestionType": 2, "Name": "南卡麦隆", "ObjectID": 534022}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI8r3HM6WIq8evvHNP8K8TGbv%2bEuUx8F0uU3Fosw4APH9DIN70VqD10qhs800q6Zj5A%2fX1FjoNrJVTpCWnrcvX45iuuVvUbFDg9h7Ynk%2fensW7%2fYcGgtFiTjzmkasArxkZoVWdRj%2fWGwVtRRBAnhaWAWArrE3SJRw3zbuLVCnJiaSxubLWWbHL40Lq76hhypS4%2bxDke5nmm4PNERdVnf5txr4vYBSd86EVFMQNW14nE%2fIg%3d%3d&selectedproperty=79104&hotel=79104&tick=636235555646", "ObjectTypeID": 7, "SuggestionType": 2, "Name": "雅温得希尔顿酒店", "ObjectID": 79104}]'}, 'tid': '16030eecf0ab4bc1d9385683991e103c', 'task_type': 3, '_id': ObjectId('5ad5ee2280e566549e47e327')}]
    cursor = sort_date_task_cursor(cursor)
    query_count = len(cursor)
    take_count = 2 if query_count >= 2 else 1

    non_breakfast_times = 0
    # take_count = 1 if query_count >= 2 else 0
    # line_list = cursor[0:2]
    try:
        for each_line in cursor[0:2]:
            package_id = each_line['package_id']
            source = each_line['task_args']['source']
            memory = memory_count_by_source.get(source, 0) + multiply_times[str(package_id)]
            if memory > 300000:
                if query_count > take_count:
                    line, take_count = decide_need_hotel_task(cursor, take_count)
            else:
                line = each_line
            source = line['task_args']['source']
            if source in ['agodaListHotel', 'elongListHotel']:
                line['need_breakfast_column'] = 0
                res.append(line)
                if query_count > take_count:
                    next_line, take_count = decide_need_hotel_task(cursor, take_count)
                    if next_line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
                        if query_count > take_count:
                            next_line, take_count = decide_need_hotel_task(cursor, take_count)
                            next_line['need_breakfast_column'] = 1
                            res.append(next_line)
                    else:
                        next_line['need_breakfast_column'] = 1
                        res.append(next_line)
            else:
                line['need_breakfast_column'] = 2
                res.append(line)

    except Exception as e:
        logger.info('{}{}'.format('+++++', cursor))
        logger.exception(msg='出错', exc_info=e)
        raise e


    return res

def query_hotel_base():
    res = []
    cursor = [
             {
                 "_id": ObjectId("5ad5ee2380e566549e47ea66"),
                 "task_type": 3,
                 "tid": "1cf166a611e510c5ca2d33ea9d522cc5",
                 "task_args": {
                     "source": "agodaListHotel",
                     "country_id": "null",
                     "suggest": "[{\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF57htlOwABYNhHHf4lNKH9iYt%2fsKxy0E9PlwQYQamWmQmF3XtrBt6d1dpRsfWZGyyG39uYvwN5W2OKVrIy6qvXAL%2bvkLuDIQXrj58SPH0Oxq89e1N2YFi5xHTc8SsTcfdn4Vu6LfAjDILIgNfhBaz5LefwzMDXWmrk6G5AmPTrC7Mmh%2fnhLzA%2bVOz06ioT5dx970FCf%2fVjEZtBKglpIVFSuacY%2fwPspU3wBrNaiQo5vZ7weIFYLKHxyFPh0lWrMycw%3d%3d&city=19557&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"亚萨瓦群岛\", \"ObjectID\": 19557}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF72wEOf0En6N%2bOW6k4lvLFEsnJupY%2fSILTmoe4eyLsdrB11P9kkYiVaLs000IKqR8EZnDlk3%2b%2fbDAMCRwRBjxhdVp13YReR7AkNGtExnl2DTxnCtuX8I%2bMb88Vp6etYRNcHzbUHALU9W2KZtgMzZStOU5VH%2fr4EeDtt7eto51aR4ufwvKP6gRGtdQ3oRVdDqFRKSsmY61cvLzcmxDji3ChxXbBJlPDcezModExOhewiv3SFoHfX5GQQ3iut%2b4HZ6FA%3d%3d&city=669661&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"塞舌尔群岛\", \"ObjectID\": 669661}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF%2fgMg06yIPGyoDPiwOgBrQXWh%2bArpqZRyexiTg5ToZvOUtAeTOPZeLhfVhC8EPJKORqs6DsRjygdWqLXv93SKup0yGlO890SVVQKupbUtmtKdcA%2bV5E1P4rO9hQDhOYwVP%2bXktMzO8a9zHKoRmLZf%2fvnoLWhk52OrbahekydB7IaC7kjPtesi%2fxxAQqYINSaINa6y1PIeTMCLVvkGQ4k5QBtLd3PNpyf6ftBl5MCuYUPOITG990FS07SZNwZvB4z4g%3d%3d&city=17759&tick=636258689889\", \"ObjectTypeID\": 5, \"SuggestionType\": 2, \"Name\": \"马尔代夫群岛\", \"ObjectID\": 17759}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOFwAw5O2SkpXpB2frNkulkfGmE0pNUfgTEAngtXSbJ7MDkcDrBVqv1aKIhvWYQBkJFO0ewg1xsraPIhgLroptl0lV6pdCzNC9QReolOLsRTs1mgkJDRWyXYs%2bhuEHG%2bLQIPjOaVAK4Ht9lzqApPhFwKoKjaqllNcOdLNPP2KhcLG%2fxX39ny2pDf2TMJwRXG8at%2bNXkMrN4L8GSQsLfM4C6EPs5t3YtLkleXGAktpRrekKVej0HbGYOsDI4qJ8gEonfA%3d%3d&area=31233&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \" 亚萨瓦岛\", \"ObjectID\": 31233}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOFyVUwGg7zyYF9gZ5Mb1kbxTD3jUCqoIl6Otu3SUQy2HnCtRIzj4JbSieu8s9sxTjdTb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaxJI8JYnZ1kv5yHlu1CSWFLO0BdvEA8oATMIgruGe7Xvgf8X1TSVstWcRZbk3B5woEY9g2a7A6ag1WYnfm7jxWQW%2fmefF6802aL64jNMttaaJAhtK0aya9Bb8XutvuFha%2fgvBnQ0XaWENBk1aLIqafg%3d%3d&area=34402&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \"吉利群岛\", \"ObjectID\": 34402}, {\"Url\": \"/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF57htlOwABYNhHHf4lNKH9iJ1BmzOWdSHZCBukoHuG7S8aqJazj7hNOqVmizYUL%2biDb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaxJI8JYnZ1kv5yHlu1CSWFPP5SZ5OuUQfNLp9cp9NCojgf8X1TSVstWcRZbk3B5woEY9g2a7A6ag1WYnfm7jxWQW%2fmefF6802aL64jNMttaaHneQUXQmFPLI7eX2u93DJljbhP%2fUeEsISSmQnbHyr%2fA%3d%3d&area=517704&tick=636258689889\", \"ObjectTypeID\": 6, \"SuggestionType\": 2, \"Name\": \"亚萨马尔\", \"ObjectID\": 517704}]",
                     "city_id": "30166",
                     "suggest_type": 0,
                     "ticket_info": {},
                     "content": ""
                 },
                 "package_id": 5
             }
,
             {
                 "_id": ObjectId("5ad5ee2380e566549e47ecf6"),
                 "task_type": 3,
                 "tid": "e3b2a9ac603eb9bf0dd4be0b7851a8bb",
                 "task_args": {
                     "source": "expediaListHotel",
                     "country_id": "null",
                     "suggest": "[{\"hierarchyInfo\": {\"country\": {\"name\": \"斐济\", \"isoCode2\": \"FJ\", \"isoCode3\": \"FJI\"}, \"airport\": {\"airportCode\": \"NAN\", \"multicity\": \"6023598\"}}, \"index\": \"0\", \"packageable\": \"false\", \"regionNames\": {\"fullName\": \"亚萨瓦群岛, 斐济\", \"lastSearchName\": \"亚萨瓦群岛, 斐济\", \"displayName\": \"<B>亚萨瓦群岛</B>, <B>斐济</B>\", \"shortName\": \"亚萨瓦群岛\"}, \"popularity\": \"7.0\", \"coordinates\": {\"lat\": \"-17.027000\", \"long\": \"177.143500\"}, \"hotelCountPerRegion\": \"15\", \"score\": \"1.0\", \"regionGrades\": {\"globalGrade\": \"0\"}, \"essId\": {\"sourceName\": \"GAI\", \"sourceId\": \"6049187\"}, \"gaiaId\": \"6049187\", \"type\": \"NEIGHBORHOOD\", \"@type\": \"gaiaRegionResult\", \"regionMetadata\": {\"hotelCount\": \"15\"}}]",
                     "city_id": "30166",
                     "suggest_type": 0,
                     "ticket_info": {},
                     "content": ""
                 },
                 "package_id": 5
             }
,
             {
                 "_id": ObjectId("5ad5ee2480e566549e47f0d6"),
                 "task_type": 3,
                 "tid": "f9dfc389f773f5c2c7637f14fdbdefbf",
                 "task_args": {
                     "source": "elongListHotel",
                     "country_id": "null",
                     "suggest": "[{\"hotelCnt\": \"1\", \"composedName\": \"亚萨瓦岛-斐济\", \"stateNameCn\": \"斐济\", \"regionId\": \"10523\", \"regionNameEn\": \"Yasawa Island\", \"countryNameCn\": \"斐济\", \"regionNameCn\": \"亚萨瓦岛\"}]",
                     "city_id": "30166",
                     "suggest_type": 0,
                     "ticket_info": {},
                     "content": ""
                 },
                 "package_id": 5
             }
,
             {
                 "_id": ObjectId("5ad5ee2480e566549e47f424"),
                 "task_type": 3,
                 "tid": "506576913115676a680893b7fa1fe04e",
                 "task_args": {
                     "source": "hotelsListHotel",
                     "country_id": "null",
                     "suggest": "[{\"name\": \"亚萨瓦岛\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.52312406, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span>, <span class='highlighted'>斐济</span>\", \"destinationId\": \"1638702\", \"latitude\": -16.74961406, \"landmarkCityDestinationId\": null, \"type\": \"CITY\", \"geoId\": \"1000000000000010523\"}, {\"name\": \"亚萨瓦岛度假村\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.53165, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span>度假村, 亚萨瓦岛, <span class='highlighted'>斐济</span>\", \"destinationId\": \"272960\", \"latitude\": -16.76407, \"landmarkCityDestinationId\": null, \"type\": \"HOTEL\", \"geoId\": \"1100000000000272960\"}, {\"name\": \"姆布卡马\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.554245, \"caption\": \"姆布卡马, <span class='highlighted'>斐济</span> (<span class='highlighted'>亚萨瓦岛</span>)\", \"destinationId\": \"1771406\", \"latitude\": -16.75961619, \"landmarkCityDestinationId\": null, \"type\": \"CITY\", \"geoId\": \"1000000000006269054\"}, {\"name\": \"亚萨瓦岛 (YAS)\", \"redirectPage\": \"DEFAULT_PAGE\", \"longitude\": 177.545556, \"caption\": \"<span class='highlighted'>亚萨瓦岛</span> (YAS), <span class='highlighted'>斐济</span>\", \"destinationId\": \"1779614\", \"latitude\": -16.758889, \"landmarkCityDestinationId\": null, \"type\": \"MINOR_AIRPORT\", \"geoId\": \"1000000000006285185\"}]",
                     "city_id": "30166",
                     "suggest_type": 0,
                     "ticket_info": {},
                     "content": ""
                 },
                 "package_id": 5
             }
    ]

    # random.shuffle(cursor)
    cursor = sort_date_task_cursor(cursor)
    query_count = len(cursor)
    take_count = 2 if query_count >= 2 else 1
    line_list = cursor[0:2]
    try:
        for line in line_list:
            if line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
                line['need_breakfast_column'] = 0
                res.append(line)
                if query_count > take_count:
                    next_line = cursor[take_count]
                    take_count += 1
                    if next_line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
                        if query_count > take_count:
                            next_line = cursor[take_count]
                            next_line['need_breakfast_column'] = 1
                            res.append(next_line)
                            take_count += 1
                    else:
                        next_line['need_breakfast_column'] = 1
                        res.append(next_line)
            else:
                line['need_breakfast_column'] = 2
                res.append(line)
    except Exception as e:
        logger.info('{}{}'.format('+++++', cursor))
        raise e
    return res

# def source_take_test():
#     res = []
#     # cursor = [{'task_args':{'source':'agodaListHotel'}},   {'task_args':{'source':'elongListHotel'}}, {'task_args':{'source':'ctripListHotel'}},{'task_args':{'source':'bookingListHotel'}}]
#     # random.shuffle(cursor)
#     cursor = [{'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'hotelsListHotel',
#                              'ticket_info': {}, 'suggest_type': 0,
#                              'suggest': '[{"name": "Yaounde", "m_url": "https://zh.hotels.com/search.do?destination-id=258832&q-check-in=2017-05-12&q-room-0-adults=2&q-destination=Yaounde%2C+Cameroon&resolved-location=CITY%3A258832%3AUNKNOWN%3AUNKNOWN&q-rooms=1&q-room-0-children=0&q-check-out=2017-05-13", "redirectPage": "DEFAULT_PAGE", "longitude": 11.53391106, "caption": "Yaounde, Cameroon", "destinationId": "258832", "latitude": 3.86740467, "landmarkCityDestinationId": null, "type": "CITY", "geoId": "1000000000000003886"}]'},
#                'task_type': 3, 'tid': '8ae3ed5e278617dbbf03b5ba2a3a3c22', '_id': ObjectId('5ad5ee2180e566549e47d805'),
#                'need_breakfast_column': 2, 'package_id': 5}, {
#                   'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'elongListHotel',
#                                 'ticket_info': {}, 'suggest_type': 0,
#                                 'suggest': '[{"hotelCnt": "27", "composedName": "雅温德(Yaounde)-喀麦隆", "m_url": "http://ihotel.elong.com/region_3886", "stateNameCn": "喀麦隆", "regionId": "3886", "regionNameEn": "Yaounde", "countryNameCn": "喀麦隆", "regionNameCn": "雅温德"}]'},
#                   'task_type': 3, 'tid': '9e20b155b55671dacb3ac12e49552653',
#                   '_id': ObjectId('5ad5ee1e80e566549e47ba15'), 'need_breakfast_column': 0, 'package_id': 5},
#               {'package_id': 5,
#                'task_args': {'content': '', 'country_id': 'null', 'city_id': '40197', 'source': 'agodaListHotel',
#                              'ticket_info': {}, 'suggest_type': 0,
#                              'suggest': '[{"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI%2b1nmDSPGC623D3eHVDv6JlN2jcr0MizlS8Y%2bIrgxTFsIyRrmZNuO4JEaDx8eznEh5Eu26pkUFDpiPe7zeq1R6UiOGQjCRqtvugSOJfYh77LiDj0Wxxs%2f8f%2bJCQUj32AzfuqTLAZLSyJ9gCyxAL1xDkd%2bYbVu0nitcgaq4HocuVY1JsdwdpdBrVkYiJUSC5POk%3d&city=8102&tick=636235555645", "ObjectTypeID": 5, "SuggestionType": 2, "Name": "雅温得", "ObjectID": 8102}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF0Xja%2ftpnb803tBpmzl%2bZ8YYLq%2bw4DI9TfT%2bgYOfyyPPZaOoeY7Skwyjupb89Me28Z0Ol6GnRgUry8P8OiXKX92hZM3PA9aTXo4PX%2b3l5Cb5kI2OY%2bkVOzT7Yatmd02nylQNjq3Y8Hr0I1wSjn%2fQd%2fe4vVNRO8X9Mb3vL5TiNZd8Bp0eoREr2xLYHgqmk0Io4N1cb46s%2fzaPNE3EidG6PjURIP3As62OoJqmINhDbrmU&city=18231&tick=636235555646", "ObjectTypeID": 5, "SuggestionType": 2, "Name": "温得和克", "ObjectID": 18231}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF0ZXX3iKqUL7oERMGEKbR2T1T51Gk8ALTYKAXe97BzKIPrUSeK1W9iNzkxom%2f0QxzEzQM6Rkwzm4hKAiNXvJk1yMka5mTbjuCRGg8fHs5xIeRLtuqZFBQ6Yj3u83qtUelIjhkIwkarb7oEjiX2Ie%2by4g49FscbP%2fH%2fiQkFI99gM37qkywGS0sifYAssQC9cQ5HfmG1btJ4rXIGquB6HLlWNii6c9DYfE%2fijR5ue6ZNle&poi=59720&tick=636235555646", "ObjectTypeID": 12, "SuggestionType": 2, "Name": "喀麦隆雅温得机场", "ObjectID": 59720}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI%2b1nmDSPGC623D3eHVDv6JlPue4UQ83VP2nWySCiPMupIyRrmZNuO4JEaDx8eznEh5Eu26pkUFDpiPe7zeq1R6UiOGQjCRqtvugSOJfYh77LiDj0Wxxs%2f8f%2bJCQUj32AzfuqTLAZLSyJ9gCyxAL1xDkd%2bYbVu0nitcgaq4HocuVYx19YSIcovR%2f0ZnvDzYvit4%3d&area=23151&tick=636235555646", "ObjectTypeID": 6, "SuggestionType": 2, "Name": "雅温得", "ObjectID": 23151}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF4EVcqAQ5AwwZdxhINUOcBQjhD37ys%2fFU6lN7cuMgW2ijI84yWSnLTvNRDJA%2fhRDwTb0O7ulaZsPbs60PaM6ANeXrJD0YdCUq%2fJwDkCvQ%2bkaIW6M7bKripp4Z%2fN%2f1mPbaPHgqJuKO9FTQewa9IJFCFdJbjP94v%2fAEU%2bFMV2YvDqAbbYhAtfVY%2bSwpUg5x%2fQA4cKA4NNzHJcSJJKF2WAjXF444SB8L40JMO9PYZz%2fFtP2&area=534022&tick=636235555646", "ObjectTypeID": 6, "SuggestionType": 2, "Name": "南卡麦隆", "ObjectID": 534022}, {"Url": "/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?asq=u2qcKLxwzRU5NDuxJ0kOF5FVrI5eLP6EmRupDeW0BI8r3HM6WIq8evvHNP8K8TGbv%2bEuUx8F0uU3Fosw4APH9DIN70VqD10qhs800q6Zj5A%2fX1FjoNrJVTpCWnrcvX45iuuVvUbFDg9h7Ynk%2fensW7%2fYcGgtFiTjzmkasArxkZoVWdRj%2fWGwVtRRBAnhaWAWArrE3SJRw3zbuLVCnJiaSxubLWWbHL40Lq76hhypS4%2bxDke5nmm4PNERdVnf5txr4vYBSd86EVFMQNW14nE%2fIg%3d%3d&selectedproperty=79104&hotel=79104&tick=636235555646", "ObjectTypeID": 7, "SuggestionType": 2, "Name": "雅温得希尔顿酒店", "ObjectID": 79104}]'},
#                'tid': '16030eecf0ab4bc1d9385683991e103c', 'task_type': 3, '_id': ObjectId('5ad5ee2280e566549e47e327')}]
#     cursor = sort_date_task_cursor(cursor)
#     query_count = len(cursor)
#     take_count = 1 if query_count >= 2 else 0
#     line_list = cursor[0:2]
#     try:
#         for line in line_list:
#             package_id = line['package_id']
#             source = line['task_args']['source']
#             memory = memory_count_by_source.get(source, 0)
#
#         if source in ['agodaListHotel', 'elongListHotel']:
#             line['need_breakfast_column'] = 0
#             res.append(line)
#             if query_count > take_count:
#                 next_line = cursor[take_count]
#                 take_count += 1
#                 if next_line['task_args']['source'] in ['agodaListHotel', 'elongListHotel']:
#                     if query_count > take_count:
#                         next_line = cursor[take_count]
#                         next_line['need_breakfast_column'] = 1
#                         res.append(next_line)
#                         take_count += 1
#                 else:
#                     next_line['need_breakfast_column'] = 1
#                     res.append(next_line)
#         else:
#             line['need_breakfast_column'] = 2
#             if memory < 300000:
#                 memory_count_by_source[source] = memory + multiply_times[str(package_id)]
#                 res.append(line)
#             else:
#                 take_count += 1
#                 decide_append_hotel_source(source, memory, cursor, line, package_id, res, query_count, take_count)
#
#     except Exception as e:
#         logger.info('{}{}'.format('+++++', cursor))
#         raise e
#
#     return res

source_take_test()
# query_hotel_base()