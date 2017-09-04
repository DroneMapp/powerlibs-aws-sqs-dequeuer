import atexit
import logging
import multiprocessing
import os
import queue
import time
import threading
from io import StringIO

import boto3
from cached_property import cached_property

from .handler import handle_message


class SQSDequeuer:
    def __init__(self,
                 queue_name, message_handler,
                 process_pool_size=2,
                 thread_pool_size=2,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_region=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.queue_name = queue_name
        self.message_handler = message_handler

        self.process_pool_size = process_pool_size
        self.thread_pool_size = thread_pool_size
        self.thread_queue = queue.Queue()
        self.threads = []
        self.alive = True

        self.aws_access_key_id = aws_access_key_id or os.environ['AWS_ACCESS_KEY_ID']
        self.aws_secret_access_key = aws_secret_access_key or os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_region = aws_region or os.environ['AWS_REGION']

        self.start_thread_pool()
        atexit.register(self.shutdown)

        self.logger.debug('New SQSDequeuer: {}'.format(self.queue_name))

    def shutdown(self):
        self.alive = False

        for t in self.threads:
            t.join()

    def __del__(self):
        self.shutdown()

    @cached_property
    def process_pool(self):
        return multiprocessing.Pool(processes=self.process_pool_size)

    def run_thread(self, thread_number):
        output = StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.getLogger('dequeuer_thread_{}'.format(thread_number))
        logger.addHandler(handler)

        while self.alive:
            try:
                entry = self.thread_queue.get(timeout=5)
            except queue.Empty:
                time.sleep(5)
                continue

            function, args, kwargs = entry

            try:
                function(logger, *args, **kwargs)
            except Exception as ex:
                t = type(ex)
                logger.error(f'Exception {t}: {ex}')  # NOQA

            output.truncate()


    def start_thread_pool(self):
        for i in range(0, self.thread_pool_size):
            t = threading.Thread(target=self.run_thread, args=[i])
            t.start()
            self.threads.append(t)

    def execute_new_process(self, function, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}

        output = StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.getLogger('dequeuer_process')
        logger.addHandler(handler)

        if self.process_pool_size:
            return self.process_pool.apply_async(function, logger, args, kwargs)

        return function(logger, *args, **kwargs)

    def execute_new_thread(self, function, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}

        if self.thread_pool_size:
            return self.thread_queue.put((function, args, kwargs))

        output = StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.getLogger('dequeuer_fake_thread')
        logger.addHandler(handler)
        return function(logger, *args, **kwargs)

    @cached_property
    def queue(self):
        return self.sqs_client.get_queue_by_name(QueueName=self.queue_name)

    @cached_property
    def sqs_client(self):  # pragma: no cover
        return boto3.resource(
            'sqs',
            self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def receive_messages(self):
        return self.queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5, VisibilityTimeout=30)

    def process_messages(self):
        messages_count = 0

        for message in self.receive_messages():
            try:
                self.handle_message(message)
            except Exception as ex:
                self.logger.warn('Exception: {ex}; message: {msg}'.format(ex=ex, msg=message))
            else:
                messages_count += 1

        self.logger.info('{} messages processed.'.format(messages_count))
        return messages_count

    def handle_message(self, message):
        self.execute_new_process(handle_message, [self.queue_name, message, self.message_handler])
