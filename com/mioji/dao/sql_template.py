#!/usr/bin/python
# coding=utf-8

CT_PARAM_HOTEL = """
/*酒店抓取城市表；包含抓取城市id、待抓取源信息*/
CREATE TABLE IF NOT EXISTS `task_param_hotel` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` char(8) NOT NULL COMMENT '任务城市id',
  `source_ids` json NOT NULL COMMENT '需抓取的源like[1,2,3] sorted DEFAULT "[]"',
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='酒店例行任务参数';
"""

CT_PARAM_FLIGHT = """
/*酒店抓取点对表；*/
CREATE TABLE IF NOT EXISTS `task_param_flight` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `dept_id` char(8) NOT NULL COMMENT '任务城市id',
  `dest_id` char(8) NOT NULL COMMENT '任务城市id',
  `source_ids` json NOT NULL COMMENT '需抓取的源like[1,2,3] DEFAULT "[]"',
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='飞机例行任务参数';
"""

CT_PACKAGE = """
/*任务包表；定义抓取任务包 任务优先级、更新周期、任务类型、任务任务参数迭代信息；他和task_param_{type}生成抓取任务表crawl_task_{type}*/
CREATE TABLE IF NOT EXISTS `package` (
  `id` int NOT NULL COMMENT '任务包id',
  `name` varchar(128) NOT NULL COMMENT '任务包名',
  `description` varchar(128) COMMENT '任务描述',
  `level` tinyint NOT NULL COMMENT '优先级',
  `period` int NOT NULL COMMENT '更新周期小时',
  `type` varchar(16) NOT NULL COMMENT '任务类型',
  `iteration` json NOT NULL COMMENT '任务迭代：like:{"occ":[2,2], "date":[7,90]}',
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务包';
"""

CT_PACKAGE_TASK_PARAM_TYPES = """
/* 任务包任务表；一个task_param_{type}可能在多个包里（迭代信息不同）*/
CREATE TABLE IF NOT EXISTS `package_task_param_{type}` (
  `package_id` int NOT NULL COMMENT '任务包id',
  `task_param_id` int NOT NULL COMMENT '任务信息参数id',
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  KEY `key_a` (`package_id`,`task_param_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务包';
"""

CT_TASK_TYPES = """
/*任务统计表；任务参数、源、任务迭代信息、该任务的统计*/
CREATE TABLE IF NOT EXISTS `crawl_task_{type}` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `task_param_id` int NOT NULL COMMENT 'task_param_id',
  `iteration_info` varchar(64) NOT NULL COMMENT '与task_param_id确定唯一抓取任务 value = package.iteration value join by : ordered key',
  `source_id` tinyint COMMENT 'source_id',
  `crawl_history_id` int NOT NULL COMMENT '上次抓取历史' DEFAULT -1,
  `last_succes_time` timestamp COMMENT '上次成功抓取时间（抓取历史有清除操作）' DEFAULT ,
  `crawl_times` int NOT NULL COMMENT '抓取次数，不依赖抓取历史' DEFAULT 0,
  `success_times` int NOT NULL COMMENT '成功次数，不依赖抓取历史' DEFAULT 0,
  `updatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `key_a` (`task_param_id`,`iteration_info`, `source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{type}任务统计表';
"""

CT_TASK_HISTORY_TYPES = """
/*抓取历史*/
CREATE TABLE IF NOT EXISTS `crawl_history_{type}` (
  `crawl_task_id` int NOT NULL COMMENT '唯一抓取任务id',
  `crawl_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `code` tinyint COMMENT '任务返回码'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{type}抓取历史';
"""

BASE_TABLE = [CT_PARAM_HOTEL, CT_PARAM_FLIGHT, CT_PACKAGE]
TYPES_TABLE = [CT_PACKAGE_TASK_PARAM_TYPES, CT_TASK_TYPES, CT_TASK_HISTORY_TYPES]
# ========= CREATE TABLE END

# INSERT SQL START
# 插入package
INSERT_PKG = 'INSERT INTO `package` (id, `name`, description, level, period, `type`, iteration) VALUES (%s,%s,%s,%s,%s,%s,%s)'
INSERT_TASK_PARAM = 'INSERT INTO task_param_{type} ({columns}) VALUES({values})'
INSERT_PACKAGE_TASK_PARAM = 'INSERT INTO package_task_param_{type} (package_id, task_param_id) VALUES(%s,%s)'
INSERT_CRAWL_TASK_PARAM = 'INSERT INTO crawl_task_{type} (task_param_id, iteration_info, source_id) VALUES(%s,%s, %s)'
#  ========= INSERT SQL END


#  ========= QUERY SQL START
QUERY_ALL_CITY = 'SELECT * FROM city WHERE status_test="Open"'
QUERY_SUGGESTED_HOTEL_CITY = 'SELECT city_id,source FROM hotel_suggestions_city where select_index >= 0'
#  ========= QUERY SQL END

