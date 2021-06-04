from google.cloud import pubsub_v1
import random
from numpy import genfromtxt
import os
import time



def generate(array):
    index = random.randint(0,len(array))
    return array[index]

while True:
    f = open("data.csv", "r", encoding="utf-8")
    str = f.read()
    my_data = str.split("\n")
    msg = generate(my_data).encode("utf-8")
    #print(msg)
    id = "gun-shooting-analysis"
    topic = "mmmmonkey"
    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{project_topic}'.format(
        project_id = id,
        project_topic = topic
    )
    future = publisher.publish(topic_name, msg)
    future.result()
    time.sleep(5)
    





