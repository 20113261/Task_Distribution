#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/5 下午10:26
# @Author  : Hou Rong
# @Site    : 
# @File    : test_mongo_offset.py
# @Software: PyCharm
import pymongo
from conf import config

collections = pymongo.MongoClient(host=config.mongo_host)[config.mongo_base_task_db]["BaseTask_Round_Flight"]
for line in collections.find({}).sort([("_id", 1)]).skip(10).limit(10):
    print(line["_id"])
    '''
None - 10
5a2684a9659e755b82883def
5a2684a9659e755b82883df0
5a2684a9659e755b82883df1
5a2684a9659e755b82883df2
5a2684a9659e755b82883df3
5a2684a9659e755b82883df4
5a2684a9659e755b82883df5
5a2684a9659e755b82883df6
5a2684a9659e755b82883df7
5a2684a9659e755b82883df8

0 - 10
5a2684a9659e755b82883def
5a2684a9659e755b82883df0
5a2684a9659e755b82883df1
5a2684a9659e755b82883df2
5a2684a9659e755b82883df3
5a2684a9659e755b82883df4
5a2684a9659e755b82883df5
5a2684a9659e755b82883df6
5a2684a9659e755b82883df7
5a2684a9659e755b82883df8
    
10 - 10
5a2684a9659e755b82883df9
5a2684a9659e755b82883dfa
5a2684a9659e755b82883dfb
5a2684a9659e755b82883dfc
5a2684a9659e755b82883dfd
5a2684a9659e755b82883dfe
5a2684a9659e755b82883dff
5a2684a9659e755b82883e00
5a2684a9659e755b82883e01
5a2684a9659e755b82883e02

15 - 10

5a2684a9659e755b82883dfe
5a2684a9659e755b82883dff
5a2684a9659e755b82883e00
5a2684a9659e755b82883e01
5a2684a9659e755b82883e02
5a2684a9659e755b82883e03
5a2684a9659e755b82883e04
5a2684a9659e755b82883e05
5a2684a9659e755b82883e06
5a2684a9659e755b82883e07

mongodb skip limit 也是左开右闭的
    '''
