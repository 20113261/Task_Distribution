#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/30 下午4:51
# @Author  : Hou Rong
# @Site    : 
# @File    : InsertBaseTask.py
# @Software: PyCharm
import pymongo
import mock
import traceback
import common.patched_mongo_insert
from conf import config
from model.TaskType import TaskType
from logger_file import get_logger
from model.BaseTask import BaseTask
from common.generate_task_info import generate_hotel_base_task_info, generate_flight_base_task_info, \
    generate_round_flight_base_task_info, generate_multi_flight_base_task_info, get_mutli_dept_airport,\
    generate_temporary_flight_task, generate_rail_base_task_info,generate_ferries_base_task_info


INSERT_WHEN = 2000


class BaseTaskList(list):
    def append_task(self, task: BaseTask):
        self.append(task.to_dict())


class InsertBaseTask(object):
    def __init__(self, task_type: TaskType, temporary_city_str=None, number=None):
        # 任务类型
        self.task_type = task_type

        self.number = number if number is not None else ''
        self.temporary_city_str = temporary_city_str

        # 初始化 collections 名称
        self.collection_name = self.generate_collection_name()

        # logger 记录日志
        self.logger = get_logger("InsertBaseTask")

        # 数据游标偏移量，用于在查询时发生异常恢复游标位置
        self.offset = 0
        # 数据游标前置偏移量，用于在入库时恢复游标位置
        self.pre_offset = 0

        client = pymongo.MongoClient(host=config.mongo_host)
        # if self.task_type == TaskType.hotel:
        #     self.db = client[config.hotel_base_task_db]
        # else:
        self.db = client[config.mongo_base_task_db]

        self.tasks = BaseTaskList()

        # 初始化建立索引
        self.create_mongo_indexes()


    def create_mongo_indexes(self):
        collections = self.db[self.collection_name]
        if self.task_type in [TaskType.Flight, TaskType.RoundFlight, TaskType.MultiFlight, TaskType.TempFlight]:
            collections.create_index([('package_id', 1)])
            collections.create_index([('tid', 1)], unique=True)
        self.logger.info("[完成索引建立]")

    def generate_collection_name(self):
        return "BaseTask_{}{}".format(str(self.task_type).split('.')[-1], self.number)

    def mongo_patched_insert(self, data):
        collections = self.db[self.collection_name]
        with mock.patch(
                'pymongo.collection.Collection._insert',
                common.patched_mongo_insert.Collection._insert
        ):
            result = collections.insert(data, continue_on_error=True)
            return result

    def insert_mongo(self):
        if len(self.tasks) > 0:
            res = self.mongo_patched_insert(self.tasks)
            self.logger.info("[update offset][offset: {}][pre offset: {}]".format(self.offset, self.pre_offset))
            self.offset = self.pre_offset
            self.logger.info("[insert info][ offset: {} ][ {} ]".format(self.offset, res))
            self.logger.info('[ 本次准备入库任务数：{0} ][ 实际入库数：{1} ][ 库中已有任务：{2} ][ 已完成总数：{3} ]'.format(
                self.tasks.__len__(), res['n'], res.get('err', 0), self.offset))

            # 入库完成，清空任务列表
            self.tasks = BaseTaskList()

    def insert_stat(self):
        """
        用于检查当前是否可以准备将任务入到 mysql 中
        :return: bool 是否准备将任务入到 mysql 中
        """
        return len(self.tasks) >= INSERT_WHEN

    def _insert_task(self, args: dict):

        if isinstance(args, dict):
            __t = BaseTask(**args)
            self.tasks.append_task(__t)
            self.pre_offset += 1
            # 如果当前可以入库，则执行入库
            if self.insert_stat():
                self.logger.info('insert into mongo:')
                self.insert_mongo()

        else:
            self.logger('错误的 args 类型 < {0} >'.format(type(args).__name__))
            raise TypeError('错误的 args 类型 < {0} >'.format(type(args).__name__))


    def insert_task(self):
        self.logger.info('insert_task:')
        i = 0
        if self.task_type == TaskType.Flight:
                for dept, dest, package_id in generate_flight_base_task_info():
                    content = "{}&{}&".format(dept, dest)
                    i += 1
                    self.logger.info(i)

                    self._insert_task(
                        {
                            'content': content,
                            'package_id': package_id,
                            # 'source': source,
                            'task_type': self.task_type
                        }
                    )

        elif self.task_type == TaskType.RoundFlight:
            for dept, dest, package_id, continent_id in generate_round_flight_base_task_info():
                content = "{}&{}&".format(dept, dest)
                self._insert_task(
                    {
                        'content': content,
                        'package_id': package_id,
                        # 'source': source,
                        'task_type': self.task_type,
                        'continent_id': continent_id
                    }
                )
        elif self.task_type == TaskType.MultiFlight:
            dept_airport_list = get_mutli_dept_airport()
            for dept_airport in dept_airport_list:
                for dept, dest, package_id, continent_id in generate_multi_flight_base_task_info():
                    content = "{}&{}|{}&{}".format(dept_airport, dept, dest, dept_airport)
                    self._insert_task(
                        {
                            'content': content,
                            'package_id': package_id,
                            # 'source': source,
                            'task_type': self.task_type,
                            'continent_id': continent_id
                        }
                    )

        elif self.task_type in [TaskType.Hotel, TaskType.TempHotel]:
            for city_id_, suggest_, suggest_type_, package_id_, country_id_, source_ in generate_hotel_base_task_info(self.temporary_city_str):
                self._insert_task(
                    {
                        'city_id': city_id_,
                        'suggest': suggest_,
                        'suggest_type': suggest_type_,
                        'package_id': package_id_,
                        'task_type': self.task_type,
                        'country_id': country_id_,
                        'source': source_
                    }
                )

        elif self.task_type == TaskType.TempFlight:
            for dept, dest, package_id in generate_temporary_flight_task(self.temporary_city_str):
                content = "{}&{}&".format(dept, dest)
                self._insert_task(
                    {
                        'content': content,
                        'package_id': package_id,
                        # 'source': source,
                        'task_type': self.task_type
                    }
                )

        elif self.task_type == TaskType.Train:
            for dept_city_id, dept_city_code, dest_city_id, dest_city_code, package_id, rail_info in generate_rail_base_task_info():
                content = dept_city_id + '&' + dept_city_code + '&' + dest_city_id + '&' + dest_city_code
                self._insert_task(
                    {
                        'content': content,
                        'package_id': package_id,
                        'task_type': self.task_type
                    }
                )

        elif self.task_type == TaskType.Ferries:
            for route_id, package_id in generate_ferries_base_task_info():
                self._insert_task(
                    {
                        'content': route_id,
                        'package_id': package_id,
                        'task_type': self.task_type
                    }
                )

        # 最终剩余数据入库
        if len(self.tasks) > 0:
            self.insert_mongo()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.insert_mongo()


if __name__ == '__main__':
    # add_city_id = '''['60184','60185','60186','60187','60188','60189','60190','60191','60192','60193','60194','60195','60196','60197','60198','60199','60200','60201','60202','60203','60204','60205','60206','60207','60208','60209','60210','60211','60212','60213','60214','60215','60216','60217','60218','60219','60220','60221','60222','60223','60224','60225','60226','60227','60228','60229','60230','60231','60232','60233','60234','60235','60236','60237','60238','60239','60240','60241','60242','60243','60244','60245','60246','60247','60248','60249','60250','60251','60252','60253','60254','60255','60256','60257','60258','60259','60260','60261','60262','60263','60264','60265','60266','60267','60268','60269','60270','60271','60272','60273','60274','60275','60276','60277','60278','60279','60280','60281','60282','60283','60284','60285','60286','60287','60288','60289','60290','60291','60292','60293','60294','60295','60296','60297','60298','60299','60300','60301','60302','60303','60304','60305','60306','60307','60308','60309','60310','60311','60312','60313','60314','60315','60316','60317','60318','60319','60320','60321','60322','60323','60324','60325','60326','60327','60328','60329','60330','60331','60332','60333','60334','60335','60336','60337','60338','60339','60340','60341','60342','60343','60344','60345','60346','60347','60348','60349','60350','60351','60352','60353','60354','60355','60356','60357','60358','60359','60360','60361','60362','60363','60364','60365','60366','60367','60368','60369','60370','60371','60372','60373','60374','60375','60376','60377','60378','60379','60380','60381','60382','60383','60384','60385','60386','60387','60388','60389','60390','60391','60392','60393','60394','60395','60396','60397','60398','60399','60400','60401','60402','60403','60404','60405','60406','60407','60408','60409','60410','60411','60412','60413','60414','60415','60416','60417','60418','60419','60420','60421','60422','60423','60424','60425','60426','60427','60428','60429','60430','60431','60432','60433','60434','60435','60436','60437','60438','60439','60440','60441','60442','60443','60444','60445','60446','60447','60448','60449','60450','60451','60452','60453','60454','60455','60456','60457','60458','60459','60460','60461','60462','60463','60464','60465','60466','60467','60468','60469','60470','60471','60472','60473','60474','60475','60476','60477','60478','60479','60480','60481','60482','60483','60484','60485','60486','60487','60488','60489','60490','60491','60492','60493','60494','60495','60496','60497','60498','60499','60500','60501','60502','60503','60504','60505','60506','60507','60508','60509','60510','60511','60512','60513','60514','60515','60516','60517','60518','60519','60520','60521','60522','60523','60524','60525','60526','60527','60528','60529','60530','60531','60532','60533','60534','60535','60536','60537','60538','60539','60540','60541','60542','60543','60544','60545','60546','60547','60548','60549','60550','60551','60552','60553','60554','60555','60556','60557','60558','60559','60560','60561','60562','60563','60564','60565','60566','60567','60568','60569','60570','60571','60572','60573','60574','60575','60576','60577','60578','60579','60580','60581','60582','60583','60584','60585','60586','60587','60588','60589','60590','60591','60592','60593','60594','60595','60596','60597','60598','60599','60600','60601','60602','60603','60604','60605','60606','60607','60608','60609','60610','60611','60612','60613','60614','60615','60616','60617','60618','60619','60620','60621','60622','60623','60624','60625','60626','60627','60628','60629','60630','60631','60632','60633','60634','60635','60636','60637','60638','60639','60640','60641','60642','60643','60644','60645','60646','60647','60648','60649','60650','60651','60652','60653','60654','60655','60656','60657','60658','60659','60660','60661','60662','60663','60664','60665','60666','60667','60668','60669','60670','60671','60672','60673','60674','60675','60676','60677','60678','60679','60680','60681','60682','60683','60684','60685','60686','60687','60688','60689','60690','60691','60692','60693','60694','60695','60696','60697','60698','60699','60700','60701','60702','60703','60704','60705','60706','60707','60708','60709','60710','60711','60712','60713','60714','60715','60716','60717','60718','60719','60720','60721','60722','60723','60724','60725','60726','60727','60728','60729','60730','60731','60732','60733','60734','60735','60736','60737','60738','60739','60740','60741','60742','60743','60744','60745','60746','60747','60748','60749','60750','60751','60752','60753','60754','60755','60756','60757','60758','60759','60760','60761','60762','60763','60764','60765','60766','60767','60768','60769','60770','60771','60772','60773','60774','60775','60776','60777','60778','60779','60780','60781','60782','60783','60784','60785','60786','60787','60788','60789','60790','60791','60792','60793','60794','60795','60796','60797','60798','60799','60800','60801','60802','60803','60804','60805','60806','60807','60808','60809','60810','60811','60812','60813','60814','60815','60816','60817','60818','60819','60820','60821','60822','60823','60824','60825','60826','60827','60828','60829','60830','60831','60832','60833','60834','60835','60836','60837','60838','60839','60840','60841','60842','60843','60844','60845','60846','60847','60848','60849','60850','60851','60852','60853','60854','60855','60856','60857','60858','60859','60860','60861','60862','60863','60864','60865','60866','60867','60868','60869','60870','60871','60872','60873','60874','60875','60876','60877','60878','60879','60880','60881','60882','60883','60884','60885','60886','60887','60888','60889','60890','60891','60892','60893','60894','60895','60896','60897','60898','60899','60900','60901','60902','60903','60904','60905','60906','60907','60908','60909','60910','60911','60912','60913','60914','60915','60916','60917','60918','60919','60920','60921','60922','60923','60924','60925','60926','60927','60928','60929','60930','60931','60932','60933','60934','60935','60936','60937','60938','60939','60940','60941','60942','60943','60944','60945','60946','60947','60948','60949','60950','60951','60952','60953','60954','60955','60956','60957','60958','60959','60960','60961','60962','60963','60964','60965','60966','60967','60968','60969','60970','60971','60972','60973','60974','60975','60976','60977','60978','60979','60980','60981','60982','60983','60984','60985','60986','60987','60988','60989','60990','60991','60992','60993','60994','60995','60996','60997','60998','60999','61000','61001','61002','61003','61004','61005','61006','61007','61008','61009','61010','61011','61012','61013','61014','61015','61016','61017','61018','61019','61020','61021','61022','61023','61024','61025','61026','61027','61028','61029','61030','61031','61032','61033','61034','61035','61036','61037','61038','61039','61040','61041','61042','61043','61044','61045','61046','61047','61048','61049','61050','61051','61052','61053','61054','61055','61056','61057','61058','61059','61060','61061','61062','61063','61064','61065','61066','61067','61068','61069','61070','61071','61072','61073','61074','61075','61076','61077','61078','61079','61080','61081','61082','61083','61084','61085','61086','61087','61088','61089','61090','61091','61092','61093','61094','61095','61096','61097','61098','61099','61100','61101','61102','61103','61104','61105','61106','61107','61108','61109','61110','61111','61112','61113','61114','61115','61116','61117','61118','61119','61120','61121','61122','61123','61124','61125','61126','61127','61128','61129','61130','61131','61132','61133','61134','61135','61136','61137','61138','61139','61140','61141','61142','61143','61144','61145','61146','61147','61148','61149','61150','61151','61152','61153','61154']'''
    # add_city_id = '''['50696']'''
    # insert_task = InsertBaseTask(task_type=TaskType.TempHotel, temporary_city_str=add_city_id, number=10004)
    # insert_task.insert_task()

    insert_task = InsertBaseTask(task_type=TaskType.Flight)
    insert_task.insert_task()