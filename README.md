# ckanext-consumer

**Event-driven architecture extension for CKAN.**

`ckanext-consumer` allows your CKAN instance to react to external events. It listens to a Kafka queue, consumes CloudEvents (or standard JSON), and dispatches them to registered Python functions in your other CKAN extensions.

## 🏗 Architecture

1.  **Consumer** acts as the worker/consumer process (`ckan consumer consume`).
2.  It discovers other plugins implementing the `IConsumer` interface.
3.  It routes incoming Kafka messages to the correct plugin based on the **Topic Name**.

## 🔧 Installation

1.  **Install the extension:**
    ```bash
    pip install -e .
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Enable the plugin** in your `ckan.ini`:
    ```ini
    ckan.plugins = ... consumer
    ```

## ⚙️ Configuration

Add the following settings to your `ckan.ini` file.

### Basic Configuration
ckan.consumer.kafka.bootstrap.servers = your-kafka-broker:9092
ckan.consumer.kafka.group_id = ckan_consumer_prod
ckan.consumer.kafka.client.id = ckan_instance_1

### Security & Authentication (SASL/SSL)
# Protocol: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
ckan.consumer.kafka.security.protocol = SASL_SSL

# Mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI (Kerberos)
ckan.consumer.kafka.sasl.mechanisms = PLAIN

# Credentials
ckan.consumer.kafka.sasl.username = your_username
ckan.consumer.kafka.sasl.password = your_password

### Tuning & Reliability
# Best practice for higher availability in librdkafka clients
ckan.consumer.kafka.session.timeout.ms = 45000
ckan.consumer.kafka.auto.offset.reset = earliest
