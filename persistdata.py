#!/usr/bin/env python

import logging
import json
import time

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
KEYSPACE = "BTC"

log.info("creating keyspace...")


session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

session.execute("""
    CREATE TABLE IF NOT EXISTS BTC.USD (
        date text PRIMARY KEY,
        Bitcoin text,
        Dollar text,
        amount text,
    )
    """)

settings = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'groupid',
    'client.id': 'clientid',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

# pricewithdate = {"date": "1527612815.19", "currency": "EUR", "amount": "6429.87", "base": "BTC"}
c = Consumer(settings)

c.subscribe(['coinbasetest'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            # print('Received message: {0}'.format(msg.value()))
            test = json.loads(msg.value())

            session.execute(
                """
                INSERT INTO BTC.USD  (date, Bitcoin, Dollar, amount)
                VALUES (%s, %s, %s, %s)
                """,
                ( str(test['date']), str(test['base']), str(test['currency']), str(test['amount']) )
            )

        elif msg.error().code() == KafkaError._PARTITION_EOF:
            '''print('End of partition reached {0}/{1}'
                 .format(msg.topic(), msg.partition()))'''
        else:
            print('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()