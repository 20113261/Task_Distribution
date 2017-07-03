#!/usr/bin/python
# coding=utf-8

import ser_excutor
import tornado.web
import functools
import traceback

app = None


def admin_authenticated(func):
    admins = {'admin': 'Mia0ji123'}

    @functools.wraps(func)
    def wrapper(*args, **kw):
        if len(args) > 0 and isinstance(args[0], tornado.web.RequestHandler):
            handler = args[0]
            if handler.get_arguments('user') and handler.get_arguments('pwd'):
                user = handler.get_arguments('user')[0]
                pwd = handler.get_arguments('pwd')[0]
                if admins.get(user, None) != pwd:
                    raise tornado.web.HTTPError(403, 'admin_authenticated requested')
                else:
                    r = func(*args, **kw)
                    return r
            else:
                raise tornado.web.HTTPError(403, 'admin_authenticated requested')
        else:
            # 非tornado请求
            raise Exception("this func is not tornado.web.RequestHandler's")

    return wrapper


def status(handler):
    """
    status
    :return: 
    """
    return app.status()


def stop(handler):
    """
    :return: 
    """
    app.stop()


def re_init_today(handler):
    """
    :return: 
    """
    app.re_init_today()
    return 'done'

def add_task(handler):
    """
    主动添加任务  
    :param task : 待添加的任务json格式字符串 
      like [{"content": "10005&2&1&20170801", "source": "agodaListHotel", "t_id": [1, 5, 1, "31:2"]},]
    :return: 
    """
    task = handler.get_arguments('task','[]')
    app.user_add()
    return


binds = {
    'status': status,
    'stop': stop,
    're_init_today': re_init_today
}

api_doc = None
api_doc_list = []
for api, func in binds.iteritems():
    doc = '<b>' + api + '</b>' + func.__doc__ + '\n\n'
    api_doc_list.append(doc)
api_doc = '<pre><b>supported admin api</b>\n\n' + ''.join(api_doc_list) + '</pre>'


class AdminHandler(tornado.web.RequestHandler):
    executor = ser_excutor.executor_pool

    @admin_authenticated
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        option = self.get_argument('option', None)
        if not option or not binds.get(option, None):
            self.write(api_doc)
        else:
            yield self.async_do(option)


    def post(self):
        option = self.get_argument('option', None)
        if not option or not binds.get(option, None):
            self.write(api_doc)
        else:
            yield self.async_do(option)

    @tornado.concurrent.run_on_executor
    def async_do(self, option):
        try:
            f = binds.get(option, None)
            r = f(self)
            self.write(r)
        except Exception, e:
            self.write('request error{0}\n{1}'.format(traceback.format_exc(), api_doc))