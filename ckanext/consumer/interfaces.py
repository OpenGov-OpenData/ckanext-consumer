from ckan.plugins.interfaces import Interface


class IConsumer(Interface):
    """Interface for registering event handlers from other extensions.

    Any plugin implementing this interface can subscribe to Kafka topics
    handled by the consumer.
    """

    def get_event_handlers(self):
        """
        Returns a dictionary mapping topic names to handler functions.

        The handler function must accept a single argument: the data payload (dict).

        Example:
            return {
                'com.opengov.users.create': self.handle_user_create,
                'com.opengov.datasets.update': self.handle_dataset_update
            }

        Returns:
            dict: { 'topic_name': callable_function }
        """
        return {}
