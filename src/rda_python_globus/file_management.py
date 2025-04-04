import sys
import click
import textwrap
import typing as t
from globus_sdk import DeleteData, GlobusAPIError, NetworkError

from .lib import (
    common_options,
    task_submission_options,
    path_options,
    endpoint_options,
    transfer_client,
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
    "mkdir",
    short_help="Create a directory on a Globus endpoint.",
    epilog='''
\b
=== Examples ===
\b
1. Create a directory on the RDA Quasar endpoint:
\b
   $ dsglobus mkdir \\
       --endpoint rda-quasar \\
       --path /d999009/new_directory
'''
)
@endpoint_options
@path_options
@common_options
def mkdir_command(
    endpoint: str,
    path: str,
) -> None:
    """
    Create a directory on a Globus endpoint. Directory path is relative to the endpoint host path.
    """
    tc = transfer_client()
    try:
        res = tc.operation_mkdir(endpoint, path=path)
        click.echo(f"{res['message']}")
    except (GlobusAPIError, NetworkError) as e:
        logger.error(f"Error creating directory: {e}")
        raise click.Abort()

@click.command(
    "rename",
    short_help="Rename a file or directory on a Globus endpoint.",
    epilog='''
\b
=== Examples ===
\b
1. Rename a single file on the RDA Quasar endpoint:
\b
   $ dsglobus rename \\
       --endpoint rda-quasar \\
       --old-path /d999009/old_file.txt \\
       --new-path /d999009/new_file.txt
\b
2. Rename a directory on the RDA Quasar endpoint:
\b
   $ dsglobus rename \\
       --endpoint rda-quasar \\
       --old-path /d999009/old_directory \\
       --new-path /d999009/new_directory
\b
3. Rename a batch of files/directories on the RDA Quasar endpoint:
\b
   $ dsglobus rename \\
       --endpoint rda-quasar \\
       --batch /path/to/batch.json
\b
   The batch file should contain a JSON array of file paths to rename.
\b
   Example batch file contents:
\b
   [
       {
           "old_path": "/d999009/file_old.txt",
           "new_path": "/d999009/file_new.txt"
       },
       {
           "old_path": "/d999009/file2_old.txt",
           "new_path": "/d999009/file2_new.txt"
       },
       {
           "old_path": "/d999009/old_directory/",
           "new_path": "/d999009/new_directory/"
       }
   ]
\b
4. The batch files can also be read from stdin using '-':
\b
   $ dsglobus rename \\
       --endpoint rda-quasar \\
       --batch -
   [
       {
           "old_path": "/d999009/file_old.txt",
           "new_path": "/d999009/file_new.txt"
       },
       {
           "old_path": "/d999009/file2_old.txt",
           "new_path": "/d999009/file2_new.txt"
       },
       {
           "old_path": "/d999009/old_directory/",
           "new_path": "/d999009/new_directory/"
       }
   ]
   <Ctrl+D>
'''
)
@click.option(
    "--old-path",
    "-op",
    type=str,
    help="Old file or directory path on the endpoint. Ignored if --batch is used.",
)
@click.option(
    "--new-path",
    "-np",
    type=str,
    help="New file or directory path on the endpoint. Ignored if --batch is used.",
)
@click.option(
    "--batch",
	type=click.File('r'),
    help=textwrap.dedent("""\
        Accept a batch of multiple file/directory name pairs from a file. 
        Use '-' to read from stdin, and close the stream with 'Ctrl+D'.  
        See examples below.
    """),
)
@endpoint_options
@common_options
def rename_command(
    endpoint: str,
    old_path: str,
    new_path: str,
    batch: t.TextIO
) -> None:
    """
    Rename a file or directory on a Globus endpoint. Path is relative to the endpoint host path.
    """
    if old_path is None and new_path is None and batch is None:
        raise click.UsageError('--old-path and --new-path, or --batch is required.')

    if batch:
        files = process_json_stream(batch)
    else:
        if old_path is None or new_path is None:
            raise click.UsageError('--old-path and --new-path are required if --batch is not used.')
        files = [
            {
                "old_path": old_path,
                "new_path": new_path
            }
        ]
    
    tc = transfer_client()
    for file in files:
        old_path = file["old_path"]
        new_path = file["new_path"]
        try:
            res = tc.operation_rename(endpoint, oldpath=old_path, newpath=new_path)
            click.echo(f"old path: {old_path}\nnew path: {new_path}\n{res['message']}")
        except (GlobusAPIError, NetworkError) as e:
            logger.error(f"Error renaming file/directory: {e}")
            raise click.Abort()

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
2. Delete a directory on the RDA Quasar endpoint.  --recursive is required
   to delete a directory and its contents:
\b
   $ dsglobus delete \\
       --endpoint rda-quasar \\
       --target-file /d999009/dir \\
       --recursive
\b
3. Delete a batch of files/directories on the RDA Quasar endpoint:
\b
   $ dsglobus delete \\
       --endpoint rda-quasar \\
       --batch /path/to/batch.json \\
       --recursive
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
       --recursive \\
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
    "--target-file",
    "-tf",
    type=str,
    help="File or directory to delete on the endpoint. Ignored if --batch is used.",
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
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    default=False,
    show_default=True,
    help="Recursively delete directories and their contents.  Required if deleting a directory.",
)
@endpoint_options
@task_submission_options
@common_options
def delete_command(
    endpoint: str,
    target_file: str,
    label: str,
    batch: t.TextIO,
    dry_run: bool,
    recursive: bool,
) -> None:
    """
    Delete files and/or directories on a Globus endpoint. Directory
    path is relative to the endpoint host path.
    """
    tc = transfer_client()
    delete_data = DeleteData(tc, endpoint, label=label, recursive=recursive)

    # If a batch file is provided, read the file and add to delete data
    if batch:
        try:
            delete_data = add_batch_to_delete_data(batch, delete_data)
        except ValueError as e:
            logger.error(f"Error processing batch file: {e}")
            raise click.Abort()
    else:
        if target_file is None:
            raise click.UsageError('--target-file is required if --batch is not used.')
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
        click.echo(f"Endpoint: {data['endpoint']}")
        try:
            click.echo(f"Label: {data['label']}")
        except KeyError:
            click.echo("Label: None")
        click.echo("Files to delete:")
        for item in data["DATA"]:
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
    group.add_command(mkdir_command)
    group.add_command(rename_command)
    group.add_command(delete_command)
