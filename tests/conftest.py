import json
from unittest import mock

import pytest

from powerlibs.aws.sqs.dequeuer import SQSDequeuer


def make_a_message(payload):
    return type(
        'MockedMessage',
        (object,),
        {
            'body': """{"Type" : "Notification",
                "MessageId" : "5236f865-61b8-5882-ba1b-a91938a3b891",
                "TopicArn" : "arn:aws:sns:us-east-1:571726798637:test_topic",
                "Message" : {payload},
                "Timestamp" : "2017-04-27T20:38:51.525Z"}""".format(payload=json.dumps(payload)),
            'delete': mock.Mock(),
        }
    )


@pytest.fixture
def message_handler():
    return mock.Mock()


@pytest.fixture
def dequeuer(message_handler):
    return SQSDequeuer(
        'TEST QUEUE',
        message_handler,
        process_pool_size=0,  # Do not use multiprocessing.
        thread_pool_size=0,  # Do not use threads.
        aws_access_key_id='AWS_ID',
        aws_secret_access_key='AWS_SECRET',
        aws_region='AWS_REGION'
    )
