#!/usr/bin/env python
import pika
import sys
from time import sleep
from datetime import datetime as dt
from MM_demo import MM


import logging
logging.basicConfig(level=logging.ERROR)


attempts = 10


def on_request(ch, method, props, context):
    # body is a JSON of request parameters to be passed

    # print('Running {}'.format(q))

    response = m.run_strat(context=context)

    ch.basic_publish(exchange='', routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)

m = MM()
while True:
    q = 'quotes'
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat_interval=60))
    channel = connection.channel()
    channel.queue_declare(queue=q)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(on_request, queue=q)
    print(" [x] Awaiting RPC requests")
    channel.start_consuming()
