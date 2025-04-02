import click
from globus_sdk import DeleteData, GlobusAPIError, NetworkError

from .lib import (
    common_options,
    transfer_client,
    validate_endpoint,
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
    "--label",
    "-l",
    type=str,
    default="",
    help="Label for the delete task.",
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
@common_options
def delete_command(
    endpoint: str,
    target_file: str,
    label: str,
    batch: t.TextIO,
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

    # Submit the task
    try:
        task_id = tc.submit_transfer(transfer_data)
        logger.info(f"Task ID: {task_id}")
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error submitting task: {e}")
        raise click.Abort()

def add_commands(group):
    group.add_command(delete_command)
