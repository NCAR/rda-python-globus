import sys
import json
import typing as t
import textwrap

import click
from globus_sdk import TransferData, GlobusAPIError, NetworkError

from .lib import (
    common_options, 
    task_submission_options,
    transfer_client,
    process_json_stream,
    validate_endpoint,
)

import logging
logger = logging.getLogger(__name__)

def add_batch_to_transfer_data(batch, transfer_data):
    """ Add batch of files to transfer data object. """

    batch_json = process_json_stream(batch)

    try:
        files = batch_json['files']
    except KeyError:
        logger.error("[add_batch_to_transfer_data] Files missing from JSON or command-line input")
        sys.exit(1)

    for i in range(len(files)):
        source_file = files[i]['source_file']
        dest_file = files[i]['destination_file']		
        transfer_data.add_item(source_file, dest_file)

    return transfer_data

@click.command(
    "transfer",
    help="Submit a Globus transfer task.",
    epilog='''
\b
=== Valid RDA endpoint names ===
- rda-glade
- rda-quasar
- rda-quasar-drdata

\b
=== Examples ===
\b
1. Transfer a single file from GLADE to the NCAR Quasar tape system:

\b
   $ dsglobus transfer \\
       --source-endpoint rda-glade \\
       --destination-endpoint rda-quasar \\
       --source-file /data/d999009/file.txt \\
       --destination-file /d999009/file.txt	  			 

2. Transfer a batch of files from a JSON file:

\b				   
   $ dsglobus transfer \\
       --source-endpoint SOURCE_ENDPOINT \\
       --destination-endpoint DESTINATION_ENDPOINT \\
       --batch /path/to/batch.json

3. Transfer multiple files with the --batch option in JSON format.  Use '-' to read from stdin, and close the stream with 'Ctrl+D':

\b
   $ dsglobus transfer \\
       --source-endpoint SOURCE_ENDPOINT \\
       --destination-endpoint DESTINATION_ENDPOINT \\
       --batch -
   {
     "files": [
       {"source_file": "/data/d999009/file1.tar", "destination_file": "/d999009/file1.tar"},
       {"source_file": "/data/d999009/file2.tar", "destination_file": "/d999009/file2.tar"},
       {"source_file": "/data/d999009/file3.tar", "destination_file": "/d999009/file3.tar"}
     ]
   }
   <Ctrl+D>
''',
)
@click.option(
    "--source-endpoint",
	"-se",
	required=True,
    callback=validate_endpoint,
    help="Source endpoint ID or name (alias).",
)
@click.option(
    "--destination-endpoint",
	"-de",
	required=True,
    callback=validate_endpoint,
    help="Destination endpoint ID or name (alias).",
)
@click.option(
	"--source-file",
    "-sf",
    default=None,
    help="Path to source file name, relative to source endpoint host path. Ignored if --batch is used.",
)
@click.option(
	"--destination-file",
    "-df",
    default=None,
    help="Path to destination file name, relative to destination endpoint host path. Ignored if --batch is used.",
)
@click.option(
	"--verify-checksum/--no-verify-checksum",
    "-vc/-nvc",
    default=True,
    show_default=True,
    help="Verify checksums of files transferred.",
)
@click.option(
	"--batch",
	type=click.File('r'),
    help=textwrap.dedent("""\
        Accept a batch of source/destination file pairs from a file. 
        Use '-' to read from stdin, and close the stream with 'Ctrl+D'.  
        Uses --source-endpoint and --destination-endpoint as passed 
        on the command line.  See examples below.
    """),
)
@common_options
@task_submission_options
def transfer_command(
    source_endpoint: str,
    destination_endpoint: str,
    source_file: str,
    destination_file: str,
    verify_checksum: bool,
    batch: t.TextIO,
    dry_run: bool,
    label: str
    ) -> None:

    if source_file is None and destination_file is None and batch is None:
        raise click.UsageError('--source-file and --destination-file, or --batch is required.')

    tc = transfer_client()
		
    transfer_data = TransferData(
        transfer_client=tc,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        label=label,
        verify_checksum=verify_checksum
    )

    if batch:
        transfer_data = add_batch_to_transfer_data(batch, transfer_data)
    else:
        if source_file is None or destination_file is None:
            raise click.UsageError('--source-file and --destination-file are required is --batch is not used.')
        transfer_data.add_item(source_file, destination_file)
		
    if dry_run:
        data = transfer_data.data
        click.echo(f"Source endpoint ID: {data['source_endpoint']}")
        click.echo(f"Destination endpoint ID: {data['destination_endpoint']}")
        try:
            click.echo(f"Label: {data['label']}")
        except KeyError:
            click.echo("Label: None")
        click.echo(f"Verify checksum: {data['verify_checksum']}")
        click.echo("Transfer items:")
        click.echo("{}".format(json.dumps(data['DATA'], indent=2)))

        # exit safely
        return

    try:
        res = tc.submit_transfer(transfer_data)
        task_id = res["task_id"]
    except GlobusAPIError as e:
        msg = ("[submit_rda_transfer] Globus API Error\n"
               "HTTP status: {}\n"
               "Error code: {}\n"
               "Error message: {}").format(e.http_status, e.code, e.message)
        logger.error(msg)
        raise e
    except NetworkError:
        logger.error("[submit_rda_transfer] Network Failure. "
               "Possibly a firewall or connectivity issue")
        raise
	
    msg = "{0}\nTask ID: {1}".format(res['message'], task_id)
    click.echo(f"""{msg}""")