import json


class DummyMsg:
    def __init__(self, topic, value):
        self._topic = topic
        self._value = value

    def topic(self):
        return self._topic

    def value(self):
        return self._value


def test_process_message_calls_handler_with_decoded_json(consumer_module):
    consumer = consumer_module

    called = {"data": None}

    def handler(data):
        called["data"] = data

    payload = {"hello": "world"}
    msg = DummyMsg("topic-a", json.dumps(payload).encode("utf-8"))

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert called["data"] == payload


def test_process_message_passes_empty_dict_for_null_value(consumer_module):
    consumer = consumer_module

    called = {"data": None}

    def handler(data):
        called["data"] = data

    msg = DummyMsg("topic-a", None)

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert called["data"] == {}


def test_process_message_logs_json_decode_error(consumer_module, caplog):
    consumer = consumer_module

    def handler(_data):
        raise AssertionError("handler should not be called")

    msg = DummyMsg("topic-a", b"not-json")

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert "JSON Decode Error" in caplog.text


def test_process_message_logs_handler_exception(consumer_module, caplog):
    consumer = consumer_module

    def handler(_data):
        raise ValueError("boom")

    msg = DummyMsg("topic-a", b"{}")

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert "Error executing handler" in caplog.text
