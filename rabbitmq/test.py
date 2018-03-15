# from tornado import gen
# from tornado.ioloop import IOLoop
# from tornado.locks import Condition
#
# condition = Condition()
#
# @gen.coroutine
# def waiter():
#     print("I'll wait right here")
#     yield condition.wait()  # Yield a Future.
#     print("I'm done waiting")
#
# @gen.coroutine
# def notifier():
#     print("About to notify")
#     condition.notify()
#     print("Done notifying")
#
# @gen.coroutine
# def runner():
#     # Yield two Futures; wait for waiter() and notifier() to finish.
#     yield [waiter(), notifier()]
#
# IOLoop.current().run_sync(runner)

# def g(x):
#     yield from range(x, 0, -1)
#     yield from range(x)
# print(list(g(5)))
# for g  in g(6):
#     print(g,end=',')
#*******************************************


from tornado import web, ioloop
import datetime
import logging
import tornado
import tornado.httpserver
from queue import Queue

period = 5 * 1000  # every 5 s

q = Queue()
q.put(1)
q.put(2)
c = q.get(1)
d = q.get(1)

class MainHandler(web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        self.write('Hello Tornado')


def like_cron():
    print(datetime.datetime.now())


def xiaorui():
    print('xiaorui 2s')


def lee():
    print('lee 3s')


# if __name__ == '__main__':
#     # logging.getLogger('tornado.access').disabled = True
#
#     application = web.Application([
#         (r'/', MainHandler),
#     ])
#     http_server = tornado.httpserver.HTTPServer(application)
#     http_server.bind(80881, '0.0.0.0')
#     http_server.start()
#     ioloop.PeriodicCallback(like_cron, period).start()  # start scheduler
#     ioloop.PeriodicCallback(lee, 3000).start()  # start scheduler
#     ioloop.IOLoop.instance().start()

from memory_profiler import profile
@profile(precision=4)
def my_func():
    a = 1
    print(a)
    del a
    a = 'a' * 1024 * 1024
    # print(a)
    del a
    a = 'a' * 1024
    # print(a)
    del a
    print("+++++++++")

if __name__ == '__main__':
    my_func()
    my_func()
    print('wanle')
#*******************************************
# hotel_source = [
#     'expediaListHotel',
#     'hotelsListHotel',
#     'bookingListHotel',
#     'elongListHotel',
#     'agodaListHotel',
#     'ctripListHotel'
# ]
#
# def is_hotel(func):
#     def wrapper():
#         for i in hotel_source:
#             func(i)
#     return wrapper
#
# @is_hotel
# def run(i):
#     print(i)
#
# run()