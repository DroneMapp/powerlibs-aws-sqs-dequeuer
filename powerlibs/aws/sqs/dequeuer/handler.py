import json
from .exceptions import MessageIgnored


def handle_message(logger, queue_name, message, handler):
    body = json.loads(message.body)
    payload = json.loads(body['Message'])

    try:
        handler(logger, queue_name, body, payload)
    except MessageIgnored:
        return

    message.delete()
