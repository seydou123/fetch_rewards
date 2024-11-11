import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from jsonschema import exceptions, validate

from user_login import USER_LOGIN_SCHEMA

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def redirect_bad_messages(msg: Dict[str, Any], error_msg: str, producer: Producer, error_topic: str):
    """Redirect messages that fail the schema validation to an error topic

    Args:
        msg: Original message
        error_msg: Error message
        producer: Kafka producer
        error_topic : Name of the error topic to send messages to
    Returns:
        Function returns nothing
    """
    # Build the message to send to the error topic
    bad_msg = {
        "user_id": msg.get("user_id"),
        "app_version": msg.get("app_version"),
        "device_type": msg.get("device_type"),
        "ip": msg.get("ip"),
        "locale": msg.get("locale"),
        "device_id": msg.get("device_id"),
        "timestamp": msg.get("timestamp"),
        "error": error_msg,
    }
    # Send the bad message to the error topic
    producer.produce(error_topic, json.dumps(bad_msg))
    producer.flush()


def format_data(
    msg: Dict[str, Any], schema: Dict[str, Any], producer: Producer
) -> Optional[Dict[str, Union[str, Any, None]]]:
    """Format user-login messages.

    Args:
        msg: Original message
        schema: User-login schema from json file
        producer: Kafka producer
    Returns:
        processed_msg : Transformed data
    """

    processed_msg = None
    try:
        # Validate schema
        validate(instance=msg, schema=schema)
        # Process the data
        processed_msg = {
            "user_id": msg.get("user_id"),
            "app_version": msg.get("app_version"),
            "device_type": msg.get("device_type"),
            "ip": msg.get("ip"),
            "locale": msg.get("locale"),
            "device_id": msg.get("device_id"),
            "created_at": datetime.fromtimestamp(int(msg.get("timestamp", 0))).strftime("%Y-%m-%dT%H:%M:%S%Z"),
            "processed_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z"),
        }

    except exceptions.ValidationError as e:
        logger.error(f"Validation error: {e.message} - Redirecting message to error topic")
        redirect_bad_messages(msg, e.message, producer, 'schema-error-user-login')

    return processed_msg


def process_messages(consumer: Consumer, producer: Producer):
    """Processes user-login messages.

    Args:
        consumer: Kafka consumer
        producer: Kafka producer
    Returns:
        Returns nothing
    """

    # Consume user-login messages and send them to a new topic
    try:
        logger.info(':::Processing messages:::')
        while True:
            msg = consumer.poll(1.0)  # Polling the message queue
            if msg is None:
                continue  # No message, keep waiting
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.warning(f"End of partition reached: {msg.partition}, offset: {msg.offset}")
                    continue
                else:
                    raise KafkaException(msg.error())

            # Process message
            processed_data = format_data(json.loads(msg.value().decode('utf-8')), USER_LOGIN_SCHEMA, producer)

            # Send processed messages to the new topic if the schema is valid
            if processed_data:
                producer.produce('processed-user-login', json.dumps(processed_data))

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
