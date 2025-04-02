import click
import logging
import logging.handlers

from . import task_management, transfer, list
from .lib import common_options, configure_log, CustomEpilog

logger = logging.getLogger(__name__)
configure_log()

@click.group("dsglobus", cls=CustomEpilog)
@common_options
def cli():
    """ 
    DSGLOBUS: A command-line tool for Globus data transfer and management of files 
    archived in the NSF NCAR Research Data Archive.
    """
    pass

# cli workflow
cli.add_command(transfer.transfer_command)
cli.add_command(list.ls_command)
task_management.add_commands(cli)