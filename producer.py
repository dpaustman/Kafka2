# coding: UTF-8
import datetime
import json
import time
import bisect

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='192.168.18.99:9092')

with open('testdata.txt', 'r') as lines:
    for line in lines:
        print(line.encode('utf-8'))
        future = producer.send('test2', line.encode('utf-8'))

    producer.close()






