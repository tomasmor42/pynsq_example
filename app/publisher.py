import os
import nsq
import tornado.ioloop
import time
from uuid import uuid4
from message_schema import MessageSchema

TOPIC = os.getenv('TOPIC', "nsq_example")
TCP_ADDRESSES = [os.getenv('TCP_ADDRESSES', '127.0.0.1:4150')]

def generate_data():
    user_id = uuid4()
    return str(user_id)

def pub_message():
    data = dict(user=generate_data(), origin="origin_website", destination="destination_website")
    schema = MessageSchema()
    message = schema.dumps(data).encode()
    writer.pub(TOPIC, message, finish_pub)

def finish_pub(conn, data):
    print(data)

if __name__ == '__main__':

    writer = nsq.Writer(TCP_ADDRESSES)
    tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
    nsq.run()