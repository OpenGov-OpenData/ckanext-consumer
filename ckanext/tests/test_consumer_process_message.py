import json


class DummyMsg:
    def __init__(self, topic, value):
        self._topic = topic
        self._value = value

    def topic(self):
        return self._topic

    def value(self):
        return self._value


def test_process_message_skips_when_no_handler(reactor_consumer_module, caplog):
    consumer = reactor_consumer_module

    msg = DummyMsg("topic-a", b"{}")
    consumer.process_message(msg, handlers={})

    assert "no handler is registered" in caplog.text


def test_process_message_calls_handler_with_decoded_json(reactor_consumer_module):
    consumer = reactor_consumer_module

    called = {"data": None}

    def handler(data):
        called["data"] = data

    payload = {"hello": "world"}
    msg = DummyMsg("topic-a", json.dumps(payload).encode("utf-8"))

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert called["data"] == payload


def test_process_message_passes_empty_dict_for_null_value(reactor_consumer_module):
    consumer = reactor_consumer_module

    called = {"data": None}

    def handler(data):
        called["data"] = data

    msg = DummyMsg("topic-a", None)

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert called["data"] == {}


def test_process_message_logs_json_decode_error(reactor_consumer_module, caplog):
    consumer = reactor_consumer_module

    def handler(_data):
        raise AssertionError("handler should not be called")

    msg = DummyMsg("topic-a", b"not-json")

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert "JSON Decode Error" in caplog.text


def test_process_message_logs_handler_exception(reactor_consumer_module, caplog):
    consumer = reactor_consumer_module

    def handler(_data):
        raise ValueError("boom")

    msg = DummyMsg("topic-a", b"{}")

    consumer.process_message(msg, handlers={"topic-a": handler})

    assert "Error executing handler" in caplog.text
