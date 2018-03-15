# -*- coding: utf-8 -*-
import sys
import init_path
import logging
import pika
import json
import time
from logger_file import get_logger
# from mysql_execute import update_monitor
# from logger_file import get_logger
from rabbitmq.producter import final_distribute, insert_mongo_data, update_running
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')
logger = get_logger('publish_hotel')


class ExamplePublisher(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """
    EXCHANGE = 'xiaopeng'
    EXCHANGE_TYPE = 'direct'
    PUBLISH_INTERVAL = 200
    QUEUE = 'hello2'
    ROUTING_KEY = 'hello2'
    QUEUE_LIST = ['agodaListHotel', 'bookingListHotel', 'ctripListHotel', 'elongListHotel', 'expediaListHotel', 'hotelsListHotel']
    MESSAGE_COUNT_LIST = {}
    IS_PUBLISHING = False
    CONFIRMATION_TIME = 0

    def __init__(self, amqp_url):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param str amqp_url: The URL for connecting to RabbitMQ

        """
        self._connection = None
        self._channel = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        self._url = amqp_url

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """
        logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     on_open_callback=self.on_connection_open,
                                     on_close_callback=self.on_connection_closed,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        logger.info('Connection opened')

        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self._connection.ioloop.stop)

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning('Channel was closed: (%s) %s', reply_code, reply_text)
        self._channel = None
        # if not self._stopping:
        #     self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info('Exchange declared')
        # self.setup_queue('cheapticketsRoundFlight')
        for queue_name in self.QUEUE_LIST:
            self.setup_queue(queue_name)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        message_count = method_frame.method.message_count
        print('message_count is %d'%message_count)
        self.MESSAGE_COUNT_LIST[method_frame.method.queue] = message_count
        logger.info('Binding %s to %s with %s',
                    self.EXCHANGE, method_frame.method.queue, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, method_frame.method.queue,
                                 self.EXCHANGE, method_frame.method.queue,)

    def on_bindok(self, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        logger.info('Queue bound')
        # self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        self.start_publish_time = time.time()
        logger.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        # self.schedule_next_message() 注释掉此，直接publish_message
        # for i in range(10):
        #     self.publish_message('wang xiao tang%d'%i)
        # self.setup_exchange(self.EXCHANGE)
        # for queue_name in self.QUEUE_LIST:
        #     # self._channel.queue_delete(callback=None, queue=queue_name, nowait=True)
        #     self.setup_queue(queue_name)
        self.publish_message()
        logger.info('*' * 30)
        self.calculate_wait_time()
        # self.start_next_publishing()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        logger.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        self.CONFIRMATION_TIME = time.time()
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        logger.info('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        logger.info('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)
        #如果为取尽
        if len(self._deliveries) == 0:
            logger.info('已取尽队列消息, 关闭连接！')
            self.stop()

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        logger.info('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def start_next_publishing(self):
        """自己定义此方法，各5分钟publish"""
        logger.info('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.add_timeout(self.PUBLISH_INTERVAL, self.start_publishing)

    def calculate_wait_time(self):
        """计算等待时间"""
        if self.CONFIRMATION_TIME:
            wait_interval = time.time() - self.CONFIRMATION_TIME
            logger.info('waited time:%d' % wait_interval)
            if wait_interval > 30:
                logger.info('超时等待，已关闭连接！')
                self.stop()
        self._connection.add_timeout(5, self.calculate_wait_time)

    def publish_message(self): #增加入队的mongo数据
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        logger.info('publish_message')
        # if self._channel is None or not self._channel.is_open:
        #     logger.info(self._channel)
        #     if (self._connection is not None and
        #             not self._connection.is_closed):
        #         # Finish closing
        #         self._connection.ioloop.stop()
        #     return

        # hdrs = {u'مفتاح': u' قيمة',
        #         u'键': u'值',
        #         u'キー': u'値'}
        # properties = pika.BasicProperties(app_id='example-publisher',
        #                                   content_type='application/json',
        #                                   headers=hdrs)
        try:
            final_distribute_result = final_distribute('Hotel')
            # final_distribute_result = {'DateTask_Round_Flight_cheapticketsRoundFlight_20180202': [(12, 824), (13, 57)]}
            logger.info(final_distribute_result)
            for queue_name, message_count in self.MESSAGE_COUNT_LIST.items():
                if message_count < 1000:
                    for collection_name, mongo_tuple_list in final_distribute_result.items():
                        for line in insert_mongo_data(queue_name, collection_name, mongo_tuple_list):
                            self.IS_PUBLISHING = True
                            try:
                                print('line: %s'% (str(line)))
                            except Exception as e:
                                pass
                            content = line['task_args']['content'] #注意往返飞机要拼接上日期，酒店不用
                            source = line['source']
                            content_list = content.split('&')
                            content_list.insert(2, source)
                            workload_key = '_'.join(content_list)
                            data = {"content": content, "error":-1,"id":line['tid'], "is_assigned":0,"priority":0,
                                    "proxy":"10.10.114.35","score":"-100","source":source, "success_times":0,
                                    "timeslot":208,"update_times":0,"workload_key": workload_key,
                                    "used_times": line['used_times'], "take_times": line['take_times'], "suggest": line['task_args']['suggest'],
                                    "suggest_type": line['task_args']['suggest_type'], "city_id": line['task_args']['city_id'],
                                    "tid":line['tid']}
                            self._channel.basic_publish('xiaopeng', queue_name,
                                                   str(data)
                                                   )
                            update_running(collection_name, line['tid'], 1)

                            self._message_number += 1
                            self._deliveries.append(self._message_number)
                            logger.info('Published message # %i', self._message_number)
                            # self.schedule_next_message()
            if self.IS_PUBLISHING is False:
                logger.info('不生产消息，马上关闭连接！')
                self.stop()
        except Exception as e:
            logger.error("发生异常", exc_info=1)

    def run(self):
        """Run the example code by connecting and then starting the IOLoop.

        """
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.add_timeout(5, self.start_publishing)

                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        logger.info('Stopped')

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        logger.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None:
            logger.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            logger.info('Closing connection')
            self._connection.close()


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    example = ExamplePublisher('amqp://hourong:1220@10.10.189.213:5672/TaskDistribute')
    example.run()


if __name__ == '__main__':
    main()