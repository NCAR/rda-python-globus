import click
import logging
import logging.handlers

from . import transfer
from .lib import common_options, configure_log

logger = logging.getLogger(__name__)
configure_log()

@click.group("dsglobus")
@common_options
def cli():
    pass

# cli workflow
cli.add_command(transfer.transfer_cli)
