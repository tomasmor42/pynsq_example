import os
import json
from json import JSONDecodeError
from marshmallow import Schema, fields, pprint
import nsq

class MesssageSchema(Schema):
    user = fields.Str()
    origin = fields.Str()
    destination = fields.Str()
    
TCP_ADDRESSES = [os.getenv('TCP_ADDRESSES', 'http://127.0.0.1:4150')]

ORIGIN = {}
DESTINATION = {}
USERS = {}

def occurance(value, value_type):
    if value in value_type.keys():
        value_type[value] += 1
    value_type[value] = 1

def handler(message):
    schema = MesssageSchema()
    try: 
        result = schema.loads(message.body.decode())
        occurance(result['user'], USERS)
        occurance(result['destination'], DESTINATION)
        occurance(result['origin'], ORIGIN)
        print(USERS)
        print(DESTINATION)
        print(ORIGIN)

        return True
    except JSONDecodeError:
        return False

r = nsq.Reader(message_handler=handler,
        nsqd_tcp_addresses=TCP_ADDRESSES,
        topic='nsq_example', 
        channel='consumer_channel', lookupd_poll_interval=15)

if __name__ == '__main__':
    nsq.run()