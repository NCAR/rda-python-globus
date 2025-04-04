import click
import uuid
import typing as t
import collections.abc
import datetime
from globus_sdk import GlobusAPIError, NetworkError

from .lib import (
    common_options,
    transfer_client,
    colon_formatted_print,
    print_table,
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

def _format_date_callback(
    ctx: click.Context | None, 
    param: click.Parameter, 
    value: datetime.datetime | None
) -> str:
    if value is None:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")

def _process_filterval(
    prefix: str,
    value: str | t.Sequence[str | uuid.UUID] | None,
    default: str | None = None,
) -> str | None:
    if not value:
        return default
    if isinstance(value, collections.abc.Sequence) and not any(value):
        return default
    if isinstance(value, str):
        return f"{prefix}:{value}"
    return f"{prefix}:{','.join(str(x) for x in value)}"

@click.command(
    short_help="Show information about a Globus task.",
)
@click.argument(
    "task-id",
    type=click.UUID,
)
@common_options
def get_task(task_id: uuid.UUID) -> None:
    """ 
    Print information including status about a Globus task.  The task may
    be pending, completed, failed, or in progress.
    """
    if not task_id:
        raise click.UsageError("TASK_ID is required.")

    tc = transfer_client()
    try:
        task_info = tc.get_task(task_id)
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error: {e}")
        click.echo("Failed to get task details.")
    if not task_info:
        click.echo("No task information available.")
        return

    fields=(
            COMMON_FIELDS
            + (COMPLETED_FIELDS if task_info["completion_time"] else ACTIVE_FIELDS)
            + (DELETE_FIELDS if task_info["type"] == "DELETE" else TRANSFER_FIELDS)
    )
    colon_formatted_print(task_info, fields)

@click.command(
    short_help="List Globus tasks."
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=10,
    help="Limit the number of results returned.",
)
@click.option(
    "--filter-task-id",
    "-ft",
    help="Comma-separated list of task IDs to filter by, formatted as UUID strings.",
)
@click.option(
    "--filter-status",
    "-fs",
    help="Comma-separated list of task status codes to filter by (ACTIVE, INACTIVE, FAILED, SUCCEEDED).",
)
@click.option(
    "--filter-type",
    "-ftt",
    help="Comma-separated list of task types to filter by (TRANSFER, DELETE).",
)
@click.option(
    "--filter-requested-before",
    "-frb",
    type=click.DateTime(),
    callback=_format_date_callback,
    help="Filter tasks requested before this date.",
)
@click.option(
    "--filter-requested-after",
    "-fra",
    type=click.DateTime(),
    callback=_format_date_callback,
    help="Filter tasks requested after this date.",
)
@click.option(
    "--filter-completed-before",
    "-fcb",
    type=click.DateTime(),
    callback=_format_date_callback,
    help="Filter tasks completed before this date.",
)
@click.option(
    "--filter-completed-after",
    "-fca",
    type=click.DateTime(),
    callback=_format_date_callback,
    help="Filter tasks completed after this date.",
)
@common_options
def task_list(
    limit: int,
    filter_task_id: str | None,
    filter_status: str | None,
    filter_type: str | None,
    filter_requested_before: str | None,
    filter_requested_after: str | None,
    filter_completed_before: str | None,
    filter_completed_after: str | None,
) -> None:
    """ 
    List the most recent Globus tasks with optional filtering.
    """    
    filter_parts = [
        _process_filterval("task_id", filter_task_id),
        _process_filterval("status", filter_status),
        _process_filterval("type", filter_type, default="type:TRANSFER,DELETE"),
    ]

    filter_parts.extend(
        [
            _process_filterval("request_time", [filter_requested_before, filter_requested_after]),
            _process_filterval("completion_time", [filter_completed_before, filter_completed_after]),
        ]
    )

    filter_string = "/".join(p for p in filter_parts if p is not None)

    fields = [
        ("Task ID", "task_id"),
        ("Status", "status"),
        ("Type", "type"),
        ("Source Display Name", "source_endpoint_display_name"),
        ("Dest Display Name", "destination_endpoint_display_name"),
        ("Request Time", "request_time"),
        ("Completion Time", "completion_time"),
        ("Label", "label")
    ]

    tc = transfer_client()
    try:
        tasks = tc.task_list(limit=limit, filter=filter_string, orderby="request_time DESC")
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error: {e}")
        click.echo("Failed to get tasks.")

    print_table(tasks, fields)

@click.command(
    help="Cancel a Globus task.",
)
@click.argument(
    "task-id",
    type=click.UUID,
)
@common_options
def cancel_task(task_id: uuid.UUID) -> None:
    """
    Cancel a Globus task.  This includes a task that is currently 
    executing or queued for execution.
    """
    if not task_id:
        raise click.UsageError("TASK_ID is required.")
    
    tc = transfer_client()
    try:
        res = tc.cancel_task(task_id)
        click.echo(f"Task {task_id}\n{res['message']}")
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error: {e}")
        click.echo("Failed to cancel task.")

def add_commands(group):
    """ Add task management commands to a click group. """
    group.add_command(get_task)
    group.add_command(task_list)
    group.add_command(cancel_task)
