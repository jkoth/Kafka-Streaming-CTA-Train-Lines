"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
from sys import exit
import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
NAMESPACE = "cta.trains.monitor"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logger.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.info("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": f"{NAMESPACE}.",
                "poll.interval.ms": 86400000,
            }
        }),
    )

    resp.raise_for_status()
    if resp.status_code == 201:
        logger.info(f"{CONNECTOR_NAME} connector created successfully")
    else:
        logger.warning(f"Error creating {CONNECTOR_NAME} Connector")
        logger.info(f"Response status code: {resp.status_code}")
        exit()

if __name__ == "__main__":
    configure_connector()
