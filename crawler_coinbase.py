#!/usr/bin/env python

import json
import time

from coinbase.wallet.client import Client
from kafka import KafkaProducer

client = Client("xxxxxxx", "XXXXXXXXX", api_version='2018-05-06')

while True:
    # Make the request to coinbase for the currency_pair ETH-EUR

    # price = {"base":"BTC","currency":"USD","amount":"7209.98"}
    price = client.get_spot_price(currency_pair = 'BTC-EUR')

    # when actual price != kafka last message read

    # pricewithdate = {"date": "1527612815.19", "currency": "EUR", "amount": "6429.87", "base": "BTC"}
    pricewithdate = '{"base":"' + str(price["base"]) + '","currency":"' + str(price["currency"]) + '","amount":"' + str(price["amount"]) + '","date":"' + str(time.time()) + '"}'

    # Insert coinbase json data in coinbasetest kafka topic
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('coinbasetest', json.loads(pricewithdate));

    # Insert every second
    time.sleep(10.0)
