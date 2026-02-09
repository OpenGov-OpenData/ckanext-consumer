class DummyKafkaError:
    _PARTITION_EOF = 123


class DummyErr:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"err({self._code})"


class DummyMsg:
    def __init__(self, value=None, err=None):
        self._value = value
        self._err = err

    def error(self):
        return self._err

    def value(self):
        return self._value


class DummyConsumer:
    def __init__(self, conf):
        self.conf = conf
        self.subscribed = None
        self.closed = False
        self.poll_calls = 0

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, _timeout):
        self.poll_calls += 1
        if self.poll_calls == 1:
            return None
        if self.poll_calls == 2:
            return DummyMsg(err=DummyErr(DummyKafkaError._PARTITION_EOF))
        if self.poll_calls == 3:
            return DummyMsg(err=DummyErr(999))
        raise KeyboardInterrupt()

    def close(self):
        self.closed = True


def test_run_consumer_exits_when_no_topics(reactor_consumer_module, monkeypatch, caplog):
    consumer = reactor_consumer_module

    # Avoid instantiating the real Consumer.
    monkeypatch.setattr(consumer, "Consumer", DummyConsumer)

    consumer.run_consumer(
        {
            "ckan.reactor.kafka.bootstrap.servers": "localhost:9092",
            "ckan.reactor.kafka.group_id": "group",
        },
        {},
    )

    assert "nothing to listen" in caplog.text


def test_run_consumer_subscribes_and_closes(reactor_consumer_module, monkeypatch):
    consumer = reactor_consumer_module

    monkeypatch.setattr(consumer, "KafkaError", DummyKafkaError)
    monkeypatch.setattr(consumer, "Consumer", DummyConsumer)

    called = {"count": 0}

    def fake_process_message(_msg, _handlers):
        called["count"] += 1

    monkeypatch.setattr(consumer, "process_message", fake_process_message)

    ckan_cfg = {
        "ckan.reactor.kafka.bootstrap.servers": "localhost:9092",
        "ckan.reactor.kafka.group_id": "group",
    }

    handlers = {"topic-a": lambda _data: None}

    consumer.run_consumer(ckan_cfg, handlers)

    assert called["count"] == 0
