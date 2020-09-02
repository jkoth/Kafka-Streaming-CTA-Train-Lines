"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    BROKER_URL = "PLAINTEXT://localhost:9092"
    REGISTRY_URL = "http://localhost:8081"
    NAMESPACE = "cta.trains.monitor"

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            "bootstrap.servers": Producer.BROKER_URL,
            "schema.registry.url": Producer.REGISTRY_URL,
            "client.id": f"{Producer.NAMESPACE}.producer",
            "compression.type": "snappy",
            "linger.ms": 5000,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 10000,
            "enable.idempotence": True,
            "acks": "all",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Instantiate Avro Producer
        try:
            self.producer = AvroProducer(self.broker_properties)
            logger.info(f"Created AvroProducer for {self.topic_name}")
        except Exception as e:
            logger.warning(f"Failed to instantiate AvroProducer for {self.topic_name}")
            logger.error(f"Error: {e}")
            exit()

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        kafka_client = AdminClient({"bootstrap.servers": Producer.BROKER_URL})
        try:
            kafka_client.create_topics([NewTopic(
                                        topic=self.topic_name,
                                        num_partitions=self.num_partitions,
                                        replication_factor=self.num_replicas,
                                        config={
                                            "cleanup.policy": "delete",  #def
                                            "retention.ms": 43200000,    #12 hrs
                                            "compression.type": "producer",
                                            "min.insync.replicas": 2,    #3 rep
                                        }
                                    )])
            logger.info(f"Created topic: {self.topic_name}")
        except Exception as e:
            logger.warning(f"Failed to create topic: {self.topic_name}")
            logger.error(f"Error: {e}")
            exit()

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info("Flushing Producer...")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
