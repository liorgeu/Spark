'''
https://rapidapi.com/weatherbit/api/weather/
'''

import json
import requests
from kafka import KafkaProducer
topic = 'Kafka-weather'
brokers ="course-kafka:9092"


url = "https://weatherbit-v1-mashape.p.rapidapi.com/current"

querystring = {"lon":"38.5","lat":"-78.5"}

headers = {
	"X-RapidAPI-Key": "5f7df78670msh470977d57fc0e58p1bfd80jsnaa924711be5b",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)
row = response.json()
print(response.text)


#########################################
producer = KafkaProducer(
	bootstrap_servers=brokers,
	client_id='producer',

	value_serializer=lambda v: json.dumps(row).encode('utf-8'))

producer = KafkaProducer(bootstrap_servers=brokers)
producer.send(topic=topic, value=json.dumps(row).encode('utf-8'))
producer.flush()

""""
client id=client id should be used to distinguish each running app against kafka. 
its a pass through to the logs. it should be set by every instance running for best fine grained monitoring, etc

acks=1==The default value is 1, which means as long as the producer 
receives an ack from the leader broker of that topic, it would take it as 
a successful commit and continue with the next message. It’s not recommended to set acks=0, 
because then you don’t get any guarantee on the commit


retries=The retries setting determines how many times the 
producer will attempt to send a message before marking it as failed.

value_serializer=How to produce Kafka messages with JSON format in Python

reconnect_backoff_ms=50
retry.backoff.ms is the time to wait before attempting to retry a failed request 
to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.

reconnect_backoff_max_ms=1000
When the Kafka cluster goes down, the KafkaClient will try to reconnect 
forever and will not fail after the reconnect_backoff_max_ms have elapsed.
Expected behavior: After reconnect_backoff_max_ms have elapsed, 
the client should fail / an exception should be thrown



"""