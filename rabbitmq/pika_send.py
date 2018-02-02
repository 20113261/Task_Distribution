import tornado.ioloop
import tornado.web
import pika
from pika.adapters.tornado_connection import TornadoConnection
import logging
import logging.config
import pymongo
from conf import task_source, config

TORNADO_PORT = 8889
# RMQ_USER = 'user'
# RMQ_PWD = 'password'
# RMQ_HOST = 'localhost'
# RMQ_PORT = 5762
#
IOLOOP_TIMEOUT = 500

# configure my logger
# logging.config.fileConfig('log.conf')

# holds channel objects

client = pymongo.MongoClient(host=config.mongo_host)
date_task_db = client[config.mongo_date_task_db]

channel = None


class PikaClient(object):
    # all the following functions precede in order starting with connect
    def connect(self):
        try:
            logger = logging.getLogger('rmq_tornado')
            username = 'hourong'  # 指定远程rabbitmq的用户名密码
            pwd = '1220'
            user_pwd = pika.PlainCredentials(username, pwd)
            param = pika.ConnectionParameters(host='10.10.189.213', virtual_host='TaskDistribute', credentials=user_pwd)

            self.connection = TornadoConnection(param, on_open_callback=self.on_connected)
        except Exception as e:
            logger.error('Something went wrong... %s', e)

    def on_connected(self, connection):
        """When we are completely connected to rabbitmq this is called"""

        # logger.info('Succesfully connected to rabbitmq')

        # open a channel
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, new_channel):
        """When the channel is open this is called"""
        logging.info('Opening channel to rabbitmq')

        global channel
        channel = new_channel
        callback = self.on_mq_declare
        for db in date_task_db.collection_names():
            queue = channel.queue_declare(queue=db,
                                     callback=callback)


    def on_mq_declare(self, frame):
        a = frame.method.message_count
        print(a)
        channel.basic_publish(exchange='', routing_key='my_queue_name',
                              body='haha')


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
        ]

        tornado.web.Application.__init__(self, handlers)


class MainHandler(tornado.web.RequestHandler):
    def post(self):
        self.write("Tornado web server.  Post a message to it using 'message' as a parameter. \
				The message will then be published to a rabbitmq queue.")


    def callback(self, ch, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        print(" [x] Received %r" % body)


    def get(self):
        try:
            self.write('已访问')
            rcv_message = self.get_argument('message', 'The received message had no content.')

            logging.info('About to send received message to rabbitmq exchange %s', rcv_message)

            # channel.basic_publish(exchange='', routing_key='my_queue_name',
            #                        body=rcv_message)

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(self.callback, queue='cheapticketsRoundFlight')
        except Exception as e:
            logger.error('Something went wrong... %s', e)


application = Application()

if __name__ == "__main__":
    application.pika = PikaClient()

    application.listen(TORNADO_PORT)

    ioloop = tornado.ioloop.IOLoop.instance()

    ioloop.add_timeout(IOLOOP_TIMEOUT, application.pika.connect)

    ioloop.start()