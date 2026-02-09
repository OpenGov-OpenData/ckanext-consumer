import sys
import types
from pathlib import Path

import pytest


class DummyKafkaError:
    _PARTITION_EOF = 123


@pytest.fixture
def extension_root():
    return Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _ensure_extension_on_syspath(extension_root):
    if str(extension_root) not in sys.path:
        sys.path.insert(0, str(extension_root))


@pytest.fixture
def reactor_consumer_module(monkeypatch):
    """Import `ckanext.reactor.consumer` with confluent_kafka stubbed."""

    dummy_kafka = types.SimpleNamespace(Consumer=object, KafkaError=DummyKafkaError)
    monkeypatch.setitem(sys.modules, "confluent_kafka", dummy_kafka)

    import importlib

    return importlib.import_module("ckanext.reactor.consumer")


@pytest.fixture
def ckan_stub(monkeypatch):
    """Provide minimal `ckan` module stubs for importing extension code."""

    class _SingletonPlugin:
        pass

    class _IClick:
        pass

    def _implements(_iface):
        return None

    class _Interface:
        pass

    plugins_mod = types.SimpleNamespace(
        SingletonPlugin=_SingletonPlugin,
        IClick=_IClick,
        implements=_implements,
        PluginImplementations=lambda _iface: [],
    )

    plugins_interfaces_mod = types.ModuleType("ckan.plugins.interfaces")
    plugins_interfaces_mod.Interface = _Interface
    toolkit_mod = types.ModuleType("ckan.plugins.toolkit")
    toolkit_mod.config = {}

    ckan_mod = types.ModuleType("ckan")

    plugins_pkg = types.ModuleType("ckan.plugins")
    plugins_pkg.__dict__.update(plugins_mod.__dict__)
    plugins_pkg.__path__ = []

    ckan_mod.plugins = plugins_pkg

    monkeypatch.setitem(sys.modules, "ckan", ckan_mod)
    monkeypatch.setitem(sys.modules, "ckan.plugins", plugins_pkg)
    monkeypatch.setitem(sys.modules, "ckan.plugins.toolkit", toolkit_mod)
    monkeypatch.setitem(sys.modules, "ckan.plugins.interfaces", plugins_interfaces_mod)

    # Also expose as attributes for `import ckan.plugins.toolkit as toolkit`.
    plugins_pkg.toolkit = toolkit_mod
    plugins_pkg.interfaces = plugins_interfaces_mod

    return types.SimpleNamespace(plugins=plugins_mod, toolkit=toolkit_mod)


@pytest.fixture
def reactor_plugin_module(monkeypatch, ckan_stub):
    """Import `ckanext.reactor.plugin` with CKAN + click + confluent stubbed."""

    # Stub `confluent_kafka` so `consumer.py` import works.
    dummy_kafka = types.SimpleNamespace(Consumer=object, KafkaError=DummyKafkaError)
    monkeypatch.setitem(sys.modules, "confluent_kafka", dummy_kafka)

    # Stub `click` so we can import plugin.py.
    def _group():
        def decorator(func):
            class _Group:
                def __init__(self, callback):
                    self.callback = callback

                def command(self):
                    def command_decorator(command_callback):
                        return types.SimpleNamespace(callback=command_callback)

                    return command_decorator

            return _Group(func)

        return decorator

    click_mod = types.SimpleNamespace(group=_group, echo=lambda _msg: None)
    monkeypatch.setitem(sys.modules, "click", click_mod)

    import importlib

    return importlib.import_module("ckanext.reactor.plugin")
