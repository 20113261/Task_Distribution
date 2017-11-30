#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 下午9:01
# @Author  : Hou Rong
# @Site    : 
# @File    : server.py
# @Software: PyCharm
# !/usr/bin/python
# coding=utf-8
import tornado.web
import tornado.ioloop
import tornado.httpserver
import json
from tornado.options import define
from conf import config

port = config.server_port
define("port", default=port, help="Run server on a specific port", type=int)
tornado.options.parse_command_line()


class GetTask(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        date_type = self.get_argument('data_type', '').strip()
        count = int(self.get_argument('count', '').strip())
        yield self.async_get(date_type.split('_'), count)

    @tornado.concurrent.run_on_executor
    def async_get(self, types, count):
        if types:
            task_list = app.get(types, count)
            self.write(json.dumps(task_list))
        else:
            self.write('[]')


class FeedBack(tornado.web.RequestHandler):
    executor = ser_excutor.executor_pool

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        task_info = self.get_argument('q', '[]').strip()
        task_list = json.loads(task_info)
        yield self.async_get(task_list)

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        task_info = self.get_argument('q', '[]').strip()
        task_list = json.loads(task_info)
        self.write('')
        yield self.async_get(task_list)

    @tornado.concurrent.run_on_executor
    def async_get(self, task_list):
        if task_list:
            app.feedback(task_list)


application = tornado.web.Application([
    (r'/workload', GetTask),
    (r'/complete_workload', FeedBack),
    (r'/admin', control.AdminHandler)
])

if __name__ == '__main__':
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(port, '0.0.0.0')
    http_server.start()
    tornado.ioloop.IOLoop.instance().start()
