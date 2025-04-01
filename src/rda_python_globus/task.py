import click
import uuid
from globus_sdk import TransferClient, GlobusAPIError, NetworkError

from .lib import (
    common_options,
    transfer_client,
    colon_formatted_print,
)

import logging
logger = logging.getLogger(__name__)

COMMON_FIELDS = [
    ("Label", "label"),
    ("Task ID", "task_id"),
    ("Is Paused", "is_paused"),
    ("Type", "type"),
    ("Directories", "directories"),
    ("Files", "files"),
    ("Status", "status"),
    ("Request Time", "request_time"),
]

ACTIVE_FIELDS = [("Deadline", "deadline"), ("Details", "nice_status")]

COMPLETED_FIELDS = [("Completion Time", "completion_time")]

DELETE_FIELDS = [
    ("Endpoint", "source_endpoint_display_name"),
    ("Endpoint ID", "source_endpoint_id"),
]

TRANSFER_FIELDS = [
    ("Source Endpoint", "source_endpoint_display_name"),
    ("Source Endpoint ID", "source_endpoint_id"),
    ("Destination Endpoint", "destination_endpoint_display_name"),
    ("Destination Endpoint ID", "destination_endpoint_id"),
    ("Bytes Transferred", "bytes_transferred"),
    ("Bytes Per Second", "effective_bytes_per_second"),
    ("Verify Checksum", "verify_checksum"),
]

SUCCESSFUL_TRANSFER_FIELDS = [
    ("Source Path", "source_path"),
    ("Destination Path", "destination_path"),
]

def print_task_detail(client: TransferClient, task_id: uuid.UUID) -> None:
    task_info = client.get_task(task_id)
    fields=(
            COMMON_FIELDS
            + (COMPLETED_FIELDS if task_info["completion_time"] else ACTIVE_FIELDS)
            + (DELETE_FIELDS if task_info["type"] == "DELETE" else TRANSFER_FIELDS)
    )
    colon_formatted_print(task_info, fields)

    return task_info.data

@click.command(
    "get-task",
    "-gt",
    short_help="Show information about a Globus task.",
)
@click.option(
    "--task-id",
    type=click.UUID,
    required=True,
    help="A Globus task ID (UUID).",
)
@common_options
def show_task_command(
    task_id: uuid.UUID,
) -> None:
    """
    Print information including status about a Globus task.  The task may
    be pending, completed, failed, or in progress.
    """

    tc = transfer_client()
    try:
        task_info = print_task_detail(tc, task_id)
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error: {e}")
        click.echo("Failed to get task details.")
