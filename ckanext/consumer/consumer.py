import logging
import sys
import json
from confluent_kafka import Consumer, KafkaError

log = logging.getLogger(__name__)


def get_kafka_config(ckan_config):
    """
    Extracts and validates Kafka configuration from CKAN .ini file.
    Supports SASL/SSL authentication and client tuning.
    """
    # 1. Base Required Configuration
    bootstrap_servers = ckan_config.get('ckan.consumer.kafka.bootstrap.servers')
    group_id = ckan_config.get('ckan.consumer.kafka.group_id')

    if not all([bootstrap_servers, group_id]):
        log.error(
            "Missing required config. Set 'ckan.consumer.kafka.bootstrap.servers' and "
            "'ckan.consumer.kafka.group_id' in ckan.ini"
        )
        sys.exit(1)

    # Initialize config with defaults
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'enable.auto.commit': True,
        'auto.offset.reset': ckan_config.get('ckan.consumer.kafka.auto.offset.reset', 'earliest'),
    }

    # 2. Extended Configuration (Security & Tuning)
    # Map CKAN .ini keys to librdkafka properties
    config_mapping = {
        # Identification
        'ckan.consumer.kafka.client.id': 'client.id',

        # Security Protocol (PLAINTEXT, SASL_SSL, etc.)
        'ckan.consumer.kafka.security.protocol': 'security.protocol',

        # SASL Auth
        'ckan.consumer.kafka.sasl.mechanisms': 'sasl.mechanisms',
        'ckan.consumer.kafka.sasl.username': 'sasl.username',
        'ckan.consumer.kafka.sasl.password': 'sasl.password',

        # Connection Tuning
        'ckan.consumer.kafka.session.timeout.ms': 'session.timeout.ms',
        'ckan.consumer.kafka.socket.timeout.ms': 'socket.timeout.ms',
    }

    # 3. Apply optional settings if they exist in .ini
    for ckan_key, kafka_key in config_mapping.items():
        value = ckan_config.get(ckan_key)
        if value:
            conf[kafka_key] = value

    return conf


def process_message(msg, handlers):
    """
    Routes the message to the correct handler based on the topic.
    Implements Resilient Error Handling strategy.
    """
    topic = msg.topic()
    msg_value = msg.value()

    handler = handlers.get(topic)

    if not handler:
        # Avoid log spam if subscribing to patterns, but useful for debugging
        log.debug(f"No handler registered for topic '{topic}'. Skipping.")
        return

    try:
        if msg_value:
            # Assumes JSON payload (CloudEvents)
            data = json.loads(msg_value.decode('utf-8'))
        else:
            data = {}

        log.info(f"⚡ Processing event from topic: {topic}")
        handler(data)

    except json.JSONDecodeError:
        log.error(f"❌ JSON Decode Error in topic '{topic}'")
    except Exception as e:
        log.error(f"❌ Error executing handler for topic '{topic}': {e}", exc_info=True)


def run_consumer(ckan_config, topic_handlers):
    """
    Main infinite loop for the Kafka consumer.
    """
    conf = get_kafka_config(ckan_config)

    # Initialize Consumer
    try:
        consumer = Consumer(conf)
    except Exception as e:
        log.error(f"Failed to initialize Kafka Consumer: {e}")
        return

    topics_to_subscribe = list(topic_handlers.keys())

    if not topics_to_subscribe:
        log.error("No topic handlers registered. Exiting.")
        return

    try:
        consumer.subscribe(topics_to_subscribe)
        log.info(f"⚛️ Consumer started as client: {conf.get('client.id', 'unknown')}")
        log.info(f"🎧 Listening on topics: {topics_to_subscribe}")

        while True:
            # Poll with 1.0s timeout
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    log.error(f"Kafka Protocol Error: {msg.error()}")
                    continue

            process_message(msg, topic_handlers)

    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
    finally:
        consumer.close()
