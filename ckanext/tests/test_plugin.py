def test_plugin_get_commands_returns_consumer_group(consumer_plugin_module):
    plugin_mod = consumer_plugin_module

    plugin = plugin_mod.ConsumerPlugin()
    commands = plugin.get_commands()

    assert commands == [plugin_mod.consumer]


def test_consume_warns_when_no_handlers(monkeypatch, consumer_plugin_module):
    plugin_mod = consumer_plugin_module

    echoed = []
    monkeypatch.setattr(plugin_mod.click, "echo", lambda msg: echoed.append(msg))

    # No plugins implementing IConsumer.
    monkeypatch.setattr(plugin_mod.plugins, "PluginImplementations", lambda _iface: [])

    # Avoid blocking consumer loop.
    called = {"conf": None, "handlers": None}

    def fake_run_consumer(conf, handlers):
        called["conf"] = conf
        called["handlers"] = handlers

    monkeypatch.setattr(plugin_mod, "run_consumer", fake_run_consumer)
    monkeypatch.setattr(plugin_mod.toolkit, "config", {"x": "y"})

    plugin_mod.consume.callback()

    assert any("No handlers registered" in m for m in echoed)
    assert called["conf"] == {"x": "y"}
    assert called["handlers"] == {}


def test_consume_loads_handlers_and_continues_on_error(monkeypatch, consumer_plugin_module):
    plugin_mod = consumer_plugin_module

    echoed = []
    monkeypatch.setattr(plugin_mod.click, "echo", lambda msg: echoed.append(msg))

    class GoodPlugin:
        name = "good_ext"

        def get_event_handlers(self):
            return {"topic-a": lambda _data: None}

    class BadPlugin:
        name = "bad_ext"

        def get_event_handlers(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(
        plugin_mod.plugins,
        "PluginImplementations",
        lambda _iface: [GoodPlugin(), BadPlugin()],
    )

    called = {"handlers": None}

    def fake_run_consumer(_conf, handlers):
        called["handlers"] = handlers

    monkeypatch.setattr(plugin_mod, "run_consumer", fake_run_consumer)
    monkeypatch.setattr(plugin_mod.toolkit, "config", {})

    plugin_mod.consume.callback()

    assert called["handlers"] == {"topic-a": called["handlers"]["topic-a"]}
    assert any("Loaded handlers" in m for m in echoed)
    assert any("Error loading handlers" in m for m in echoed)
    assert any("Total topics monitored" in m for m in echoed)
