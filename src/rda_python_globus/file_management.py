import sys
import click
import textwrap
import typing as t
from globus_sdk import DeleteData, GlobusAPIError, NetworkError

from .lib import (
    common_options,
    task_submission_options,
    transfer_client,
    validate_endpoint,
    process_json_stream,
)

import logging
logger = logging.getLogger(__name__)

def add_batch_to_delete_data(batch, delete_data):
    """ Add batch of files to delete data object. """

    delete_files = process_json_stream(batch)
    for file in delete_files:
        delete_data.add_item(file)

    return delete_data

@click.command(
    "delete",
    short_help="Delete files and/or directories on a Globus endpoint.",
    epilog='''
\b
=== Examples ===
\b
1. Delete a single file on the RDA Quasar endpoint:
\b
$ dsglobus delete \\
    --endpoint rda-quasar \\
    --target-file /d999009/file.txt

\b
2. Delete a directory on the RDA Quasar endpoint:
\b
$ dsglobus delete \\
    --endpoint rda-quasar \\
    --target-file /d999009/dir
\b
3. Delete a batch of files/directories on the RDA Quasar endpoint:
\b
$ dsglobus delete \\
    --endpoint rda-quasar \\
    --batch /path/to/batch.json
\b
The batch file should contain a JSON array of file paths to delete.
\b
Example batch file contents:
\b
[
    "/d999009/file1.txt",
    "/d999009/file2.txt",
    "/d999009/dir1",
    "/d999009/dir2"
]
\b
4. The batch files can also be read from stdin using '-':
\b
$ dsglobus delete \\
    --endpoint rda-quasar \\
    --batch -
[
    "/d999009/file1.txt",
    "/d999009/file2.txt",
    "/d999009/dir1",
    "/d999009/dir2"
]
<Ctrl+D>
'''
)
@click.option(
    "--endpoint",
    "-ep",
    type=str,
    required=True,
    callback=validate_endpoint,
    help="Endpoint ID or name (alias).",
)
@click.option(
    "--target-file",
    "-tf",
    type=str,
    required=True,
    help="File or directory to delete on the endpoint.",
)
@click.option(
	"--batch",
	type=click.File('r'),
    help=textwrap.dedent("""\
        Accept a batch of files/directories from a file. 
        Use '-' to read from stdin, and close the stream with 'Ctrl+D'.  
        See examples below.
    """),
)
@task_submission_options
@common_options
def delete_command(
    endpoint: str,
    target_file: str,
    label: str,
    batch: t.TextIO,
    dry_run: bool,
) -> None:
    """
    Delete files and/or directories on a Globus endpoint. Directory
    path is relative to the endpoint host path.
    """
    tc = transfer_client()
    delete_data = DeleteData(tc, endpoint, label=label)

    # If a batch file is provided, read the file and add to delete data
    if batch:
        try:
            delete_data = add_batch_to_delete_data(batch, delete_data)
        except ValueError as e:
            logger.error(f"Error processing batch file: {e}")
            raise click.Abort()
    else:
        # Add the target file to delete data
        try:
            delete_data.add_item(target_file)
        except ValueError as e:
            logger.error(f"Error adding target file: {e}")
            raise click.Abort()

    # If dry run is specified, print the delete data and exit
    if dry_run:
        click.echo("Dry run: delete data to be submitted:")
        data = delete_data.data
        click.echo(f"Endpoint: {data['endpoint_id']}")
        click.echo(f"Label: {data['label']}")
        click.echo("Files to delete:")
        for item in data["items"]:
            click.echo(f"  {item}")
        click.echo("\n")

        # exit safely
        sys.exit(1)

    # Submit the task
    try:
        delete_response = tc.submit_delete(delete_data)
        task_id = delete_response["task_id"]
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error submitting task: {e}")
        raise click.Abort()
    click.echo(f'Task ID: {task_id}\n{delete_response["message"]}')

def add_commands(group):
    group.add_command(delete_command)
