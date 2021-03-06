"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    BROKER_URL = "PLAINTEXT://localhost:9092"
    NAMESPACE = "cta.trains.monitor"

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.broker_properties = {
                "bootstrap.servers": KafkaConsumer.BROKER_URL,
                "group.id": f"{self.topic_name_pattern}"
        }
        self.broker_properties = {
                "bootstrap.servers": KafkaConsumer.BROKER_URL,
                "group.id": f"{self.topic_name_pattern}"
        }

        # Instantiate AvroConsumer/Consumer 
        # Use AvroConsumer for Avro formatted stream
	if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(config=self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Topic subscription with conditional earliest offset
	if self.offset_earliest:
            self.consumer.subscribe(topics=[self.topic_name_pattern] 
                                  , on_assign=self.on_assign)
        else:
            self.consumer.subscribe(topics=[self.topic_name_pattern])

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            try:
                partition.offset=0     #0-earliest, 1-current, 2-latest
                logger.info(f"Parition offset set")
            except Exception as e:
                logger.error(f"{e}")

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        logger.info("Processing poll")
        
        message = self.consumer.poll(timeout=1.0)
        if message is None:
            return 0
        elif message.error() is not None:
            logger.debug(f"message error: {message.error()}")
            return 0
        else:
            self.message_handler(message)
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()

