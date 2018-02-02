from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Condition

condition = Condition()

@gen.coroutine
def waiter():
    print("I'll wait right here")
    yield condition.wait()  # Yield a Future.
    print("I'm done waiting")

@gen.coroutine
def notifier():
    print("About to notify")
    condition.notify()
    print("Done notifying")

@gen.coroutine
def runner():
    # Yield two Futures; wait for waiter() and notifier() to finish.
    yield [waiter(), notifier()]

IOLoop.current().run_sync(runner)