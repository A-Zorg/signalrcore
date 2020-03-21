from datetime import datetime
from enum import Enum

import msgpack
from msgpack.ext import Timestamp

from ..messages.message_type import MessageType
from ..messages.stream_item_message import StreamItemMessage
from ..messages.completion_message import CompletionMessage

from .base_hub_protocol import BaseHubProtocol

# [1, Headers, InvocationId, NonBlocking, Target, [Arguments]]
# [2, Headers, InvocationId, Item]
# [3, Headers, InvocationId, ResultKind, Result?]
# [4, Headers, InvocationId, Target, [Arguments]]
# [5, Headers, InvocationId]
# [6]
# [7, Error]

class Dynamic:

    def __init__(self, data):

        items = data.__dict__.items() if hasattr(data, "__dict__") else data.items()

        for key, value in items:

            if isinstance(value, dict):
                setattr(self, key, Dynamic(value))
                
            elif isinstance(value, list):
                convertedValue = [Dynamic(element) if isinstance(element, dict) else element for element in value]
                setattr(self, key, convertedValue)

            else:
                setattr(self, key, value)

def encode_message(message):

    priority = [
        "type",
        "headers",
        "invocation_id",
        "target",
        "arguments",
        "item",
        "result_kind",
        "result",
        "stream_ids"
    ]

    if hasattr(message, "arguments"):
        for i, argument in enumerate(message.arguments):

            if isinstance(argument, datetime):
                date_time = message.arguments[i]
                timestamp = date_time.timestamp()
                seconds = int(timestamp)
                nanoseconds = int((timestamp - int(timestamp)) * 1e9)
                message.arguments[i] = Timestamp(seconds, nanoseconds)

            elif isinstance(argument, Enum):
                message.arguments[i] = message.arguments[i].name

    result = []

    for attribute in priority:
        if hasattr(message, attribute):
            if (attribute == "type"):
                result.append(getattr(message, attribute).value)    
            else:
                result.append(getattr(message, attribute))

    return result

    # dynamic = Dynamic(message)
    # return dynamic

def decode_message(raw):

    if (raw[0] == 2):
        return StreamItemMessage(raw[1], raw[2], raw[3])
    elif (raw[0] == 3):
        # add error
        return CompletionMessage(raw[1], raw[2], raw[3], None)

    raise Exception("Unknown message type.")


class MessagepackProtocol(BaseHubProtocol):
    def __init__(self):
        super(MessagepackProtocol, self).__init__(
            "messagepack", 1, "Text", chr(0x1E))

    def parse_messages(self, raw):
        return [decode_message(msgpack.unpackb(raw[1:]))]

    def encode(self, message):
        encoded_message = msgpack.packb(message, default=encode_message)
        varint_length = self.varint(len(encoded_message))
        return varint_length + encoded_message

    def varint(self, number):
        buf = b''
        while True:
            towrite = number & 0x7f
            number >>= 7
            if number:
                buf += bytes((towrite | 0x80, ))
            else:
                buf += bytes((towrite, ))
                break
        return buf
