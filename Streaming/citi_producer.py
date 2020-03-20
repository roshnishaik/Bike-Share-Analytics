from kafka import KafkaProducer
import json
import requests
import yaml

topicName = 'citi1'

producer = KafkaProducer(bootstrap_servers=['199.60.17.210:9092', '199.60.17.193:9092'])
while True:
    r=requests.get('https://feeds.citibikenyc.com/stations/stations.json')
    data=json.dumps(r.json())
    producer.send(topicName, data.encode())
