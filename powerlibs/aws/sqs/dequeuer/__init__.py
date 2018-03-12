import atexit
import logging
import multiprocessing
import os
import queue
import sys
import time
import threading
import traceback

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

        threads_queue_maxsize = thread_pool_size * 200
        self.thread_queue = queue.Queue(maxsize=threads_queue_maxsize)
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

    def run_thread(self, index):
        msg = f'Thread {index} waiting for thread_queue'
        while self.alive:
            self.logger.debug(msg)
            try:
                entry = self.thread_queue.get(timeout=5)
            except queue.Empty:
                time.sleep(5)
                continue

            function, args, kwargs = entry
            function(*args, **kwargs)

    def start_thread_pool(self):
        for i in range(0, self.thread_pool_size):
            t = threading.Thread(target=self.run_thread, args=(i,))
            t.start()
            self.threads.append(t)

    def execute_new_process(self, function, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}

        if self.process_pool_size:
            return self.process_pool.apply_async(function, args, kwargs)

        return function(*args, **kwargs)

    def execute_new_thread(self, function, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}

        if self.thread_pool_size:
            return self.thread_queue.put((function, args, kwargs))

        return function(*args, **kwargs)

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

    def receive_messages(self, max_number_of_messages=None, visilibity_timeout=30):
        max_number_of_messages = max_number_of_messages or self.thread_pool_size
        max_number_of_messages = min(10, max_number_of_messages)
        return self.queue.receive_messages(
            MaxNumberOfMessages=max_number_of_messages,
            WaitTimeSeconds=5,
            VisibilityTimeout=visilibity_timeout,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )

    def process_messages(self):
        messages_count = 0

        for message in self.receive_messages():
            try:
                self.handle_message(message)
            except Exception as ex:
                type_, value_, traceback_ = sys.exc_info()
                self.logger.warn(f'Exception {type_}: {value_}; message: {message.body}')
                formatted_traceback = traceback.format_tb(traceback_)
                self.logger.warn(f'Traceback: {formatted_traceback}')
            else:
                messages_count += 1

        if messages_count:
            self.logger.debug('{} messages processed.'.format(messages_count))
        return messages_count

    def handle_message(self, message):
        self.execute_new_process(handle_message, [self.queue_name, message, self.message_handler])
