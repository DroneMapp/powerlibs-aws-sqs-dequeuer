import logging
import multiprocessing
import os

import boto3
from cached_property import cached_property

from .handler import handle_message


class SQSDequeuer:
    def __init__(self, queue_name, message_handler, process_pool_size=2,
                 aws_access_key_id=None, aws_secret_access_key=None, aws_region=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.queue_name = queue_name
        self.message_handler = message_handler
        self.process_pool_size = process_pool_size

        self.aws_access_key_id = aws_access_key_id or os.environ['AWS_ACCESS_KEY_ID']
        self.aws_secret_access_key = aws_secret_access_key or os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_region = aws_region or os.environ['AWS_REGION']

        self.logger.debug('New SQSDequeuer: {}'.format(self.queue_name))

    @cached_property
    def pool(self):
        return multiprocessing.Pool(processes=self.process_pool_size)

    def execute(self, function, args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}

        if self.process_pool_size:
            return self.pool.apply_async(function, args, kwargs)

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
        self.execute(handle_message, [self.queue_name, message, self.message_handler])
