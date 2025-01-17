# -*- coding: utf-8 -*-
"""
Created on Sun Jan  5 11:35:04 2025

@author: chandan.pr
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import requests
import time
import logging

# Python logic to derive yetsreday's date
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
URL = 'https://stream.wikimedia.org/v2/stream/recentchange'
KAFKA_BROKER = ['34.138.94.155:9092']
TOPIC = 'wikipedia-topic-1'

# Create a logger
logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def initializeKafkaProducer():
    kafkaProducer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                                  value_serializer=lambda v: v.encode("utf-8"),
                                  retries=5,
                                  acks="all"
                                 )
    return kafkaProducer


# Fetch data from the source URL
def pushToKafkaTopic(originalTime,producer):
    maxTime = originalTime
    try:
        response = requests.get(URL, stream=True, timeout=30)
        response.raise_for_status()
        if(response.status_code == 200):
            for line in response.iter_lines(decode_unicode=True):
                if(line.startswith("data: ")):
                    line = line.replace("data: ", "")
                    res = json.loads(line)
                    resEpochTs = res.get('timestamp')
                    resTs = datetime.fromtimestamp(resEpochTs)
                    if resTs >= originalTime:
                        maxTime = resTs
                        logger.info(f"file_timestamp:: {resTs}")
                        logger.info(f"file_epoch_timestamp:: {resEpochTs}")
                        
                        producer.send(TOPIC, line)
                        producer.flush()
                        
                    else:
                        break
    except requests.RequestException as e:
        logger.error(f"HTTP request error: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in push_to_kafka_topic: {str(e)}")
        raise
            
    return maxTime

    
def process_and_send_messages():
    try:
        originalTime = datetime.now() - timedelta(minutes=1)
        producer = initializeKafkaProducer()
       
        #fetch data from API
        while True:
            logger.info(f"original_timestamp:: {originalTime}")
            time.sleep(10)
            maxTime = pushToKafkaTopic(originalTime, producer)
            
            logger.info(f"maxTime:: {maxTime}")
            originalTime = maxTime
            
    except Exception as e:
        logger.error(f"Error sending messages:: {e}")
        raise
   
   

with DAG(dag_id='Wiki_To_Kafka',
         catchup=False,
         schedule_interval=None,
         default_args=default_args
         ) as dag:
    python_task = PythonOperator(
        task_id='stream_kafka_message',
        python_callable=process_and_send_messages
        )

python_task