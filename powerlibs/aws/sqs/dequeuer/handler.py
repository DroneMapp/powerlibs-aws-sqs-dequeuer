import json
from .exceptions import MessageIgnored


def handle_message(queue_name, message, handler):
    body = json.loads(message.body)
    payload = json.loads(body['Message'])

    try:
        handler(queue_name, body, payload)
    except MessageIgnored:
        return

    message.delete()
