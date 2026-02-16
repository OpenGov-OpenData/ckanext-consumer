def test_iconsumer_default_handlers_is_empty_dict(ckan_stub):
    from ckanext.consumer.interfaces import IConsumer

    iface = IConsumer()
    assert iface.get_event_handlers() == {}
