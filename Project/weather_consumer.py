'''
https://rapidapi.com/weatherbit/api/weather/
'''
from kafka import KafkaConsumer

# In this example we will illustrate a simple producer-consumer integration

bootstrapServers = "course-kafka:9092"
topic = 'Kafka-weather'

# First we set the consumer,
consumer = KafkaConsumer(topic,bootstrap_servers=bootstrapServers)

# print the value of the consumer
# we run the consumer to fetch the message scoming from topic1.
for message in consumer:
    print(str(message.value))