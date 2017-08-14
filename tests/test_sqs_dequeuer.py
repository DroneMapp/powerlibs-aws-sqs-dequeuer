from unittest import mock


def test_dequeuer(dequeuer, valid_message):
    dequeuer.receive_messages = mock.Mock(return_value=[valid_message])

    message_count = dequeuer.process_messages()

    assert message_count == 1
    assert dequeuer.message_handler.call_count == 1
    assert valid_message.delete.call_count == 1


def test_dequeuer_with_empty_message(dequeuer):
    dequeuer.receive_messages = mock.Mock(return_value=[""])

    message_count = dequeuer.process_messages()

    assert message_count == 0


def test_dequeuer_with_none(dequeuer):
    dequeuer.receive_messages = mock.Mock(return_value=[None])

    message_count = dequeuer.process_messages()

    assert message_count == 0
