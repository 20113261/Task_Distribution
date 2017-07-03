#!/usr/bin/python
# coding=utf-8
import logging

# 配置日志
FORMAT = "%(asctime)-15s %(threadName)s %(filename)s:%(lineno)d %(levelname)s %(message)s"
formatter = logging.Formatter(FORMAT)
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

logger = logging.getLogger('task')