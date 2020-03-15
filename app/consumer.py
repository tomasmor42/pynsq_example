import os
from json import JSONDecodeError
import nsq

from message_schema import MessageSchema
    
TCP_ADDRESSES = [os.getenv('TCP_ADDRESSES', '127.0.0.1:4150')]
TOPIC = os.getenv("TOPIC", "nsq_example")


ORIGIN = {}
DESTINATION = {}
USERS = {}

def occurance(value, value_type):
    if value in value_type.keys():
        value_type[value] += 1
        return value_type[value]
    value_type[value] = 1
    return 1

def handler(message):
    schema = MessageSchema()
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
        topic=TOPIC, 
        channel='consumer_channel', lookupd_poll_interval=15)

if __name__ == '__main__':
    nsq.run()