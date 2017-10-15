import pika
import json
import random
import pandas as pd
import time
import uuid
from datetime import datetime as dt


data = pd.read_csv('Px_sample.csv')


class MMclient(object):
    """Client for RabbitMQ to call"""
    def __init__(self, routing_key):
        self.routing_key = routing_key
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        print(n)
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=self.routing_key,
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.corr_id,),
                                   body=str(n))

        while self.response is None:
            self.connection.process_data_events()
        return self.response

rpc = MMclient(routing_key='quotes')

while True:
    # Pick a random point in time to submit quote
    t = random.randint(0, 200) / 100
    time.sleep(t)
    ts = dt.now()
    h = dt(year=ts.year, month=ts.month, day=ts.day, hour=ts.hour)
    ix = (ts - h).total_seconds()
    ix = round(ix*4)

    last_trade = data.iloc[ix]['Trade Price']

    x = random.randint(0, 100) / 100000

    bid = last_trade * (1 - x)
    ask = last_trade * (1 + x)

    context = json.dumps({'Bid': bid,
                          'Ask': ask,
                          'timestamp': ts.isoformat(),
                          'Trade Price': last_trade})
    rpc.call(n=context)

    # print(last_trade)
