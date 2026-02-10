import click
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.consumer.interfaces import IConsumer
from ckanext.consumer.consumer import run_consumer


class ConsumerPlugin(plugins.SingletonPlugin):
    """CKAN Consumer: Event-driven infrastructure plugin.

    1. Registers the 'ckan consumer consume' CLI command.
    2. Collects event handlers from other plugins via IConsumer.
    """

    plugins.implements(plugins.IClick)

    def get_commands(self):
        return [consumer]


@click.group()
def consumer():
    """Commands for the Consumer event system."""


@consumer.command()
def consume():
    """Starts the Kafka consumer worker process."""
    click.echo("Initializing Consumer environment...")

    topic_handlers = {}

    # --- Dynamic Handler Registration ---
    # Iterate over all active plugins that implement IConsumer
    for plugin in plugins.PluginImplementations(IConsumer):
        try:
            handlers = plugin.get_event_handlers()
            if handlers:
                click.echo(f"   > Loaded handlers from extension '{plugin.name}': {list(handlers.keys())}")
                topic_handlers.update(handlers)
        except Exception as e:
            click.echo(f"   ! Error loading handlers from {plugin.name}: {e}")

    if not topic_handlers:
        click.echo("⚠️  No handlers registered. The worker will start but will be idle.")
    else:
        click.echo(f"✅ Total topics monitored: {len(topic_handlers)}")

    # Load CKAN config
    conf = toolkit.config

    # Start the consumer loop (blocking)
    run_consumer(conf, topic_handlers)
