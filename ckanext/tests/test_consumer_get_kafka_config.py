import pytest


def test_get_kafka_config_requires_bootstrap_and_group_id(consumer_module, caplog):
    consumer = consumer_module

    with pytest.raises(SystemExit) as excinfo:
        consumer.get_kafka_config({})

    assert excinfo.value.code == 1
    assert "Missing required config" in caplog.text


def test_get_kafka_config_uses_default_offset_reset(consumer_module):
    consumer = consumer_module

    cfg = {
        "ckan.consumer.kafka.bootstrap.servers": "localhost:9092",
        "ckan.consumer.kafka.group_id": "group",
    }

    kafka_cfg = consumer.get_kafka_config(cfg)

    assert kafka_cfg["bootstrap.servers"] == "localhost:9092"
    assert kafka_cfg["group.id"] == "group"
    assert kafka_cfg["auto.offset.reset"] == "earliest"
    assert kafka_cfg["enable.auto.commit"] is True


def test_get_kafka_config_allows_override_offset_reset(consumer_module):
    consumer = consumer_module

    cfg = {
        "ckan.consumer.kafka.bootstrap.servers": "localhost:9092",
        "ckan.consumer.kafka.group_id": "group",
        "ckan.consumer.kafka.auto.offset.reset": "latest",
    }

    kafka_cfg = consumer.get_kafka_config(cfg)

    assert kafka_cfg["auto.offset.reset"] == "latest"
