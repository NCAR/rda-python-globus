import json
import os

import click

from .lib import (
    common_options, 
    transfer_client, 
	TRANSFER_SOURCE,
    TRANSFER_DESTINATION,
)

import logging
logger = logging.getLogger(__name__)

DEFAULT_LABEL = "RDA Quasar transfer"

def submit_transfer(data):
	""" General data transfer to RDA endpoints.  Input should be JSON formatted input 
	    if transferring multiple files in batch mode. """

	try:
		files = data['files']
	except KeyError:
		logger.error("[submit_rda_transfer] Files missing from JSON or command-line input")
		sys.exit(1)

	tc = transfer_client()
		
	transfer_data = TransferData(transfer_client=tc,
							     source_endpoint=source_endpoint,
							     destination_endpoint=destination_endpoint,
							     label=label,
							     verify_checksum=verify_checksum)

	for i in range(len(files)):
		source_file = files[i]['source_file']
		dest_file = files[i]['destination_file']		
		transfer_data.add_item(source_file, dest_file)

	try:
		response = tc.submit_transfer(transfer_data)
		task_id = response['task_id']
	except GlobusAPIError as e:
		msg = ("[submit_rda_transfer] Globus API Error\n"
		       "HTTP status: {}\n"
		       "Error code: {}\n"
		       "Error message: {}").format(e.http_status, e.code, e.message)
		logger.error(msg)
		raise e
	except NetworkError:
		logger.error(("[submit_rda_transfer] Network Failure. "
                   "Possibly a firewall or connectivity issue"))
		raise
	
	msg = "{0}\nTask ID: {1}".format(response['message'], task_id)
	logger.info(msg)
	print(msg)
	
	return response

@click.command(
    "transfer",
    help="Submit a Globus transfer task.",
)
@click.option(
    "--source-endpoint",
	"-se",
    default=TRANSFER_SOURCE,
    show_default=True,
	required=True,
    help="Source endpoint ID or name.",
)
@click.option(
    "--destination-endpoint",
	"-de",
    default=TRANSFER_DESTINATION,
    show_default=True,
	required=True,
    help="Destination endpoint ID or name.",
)
@click.option(
	"--source-file",
    "-sf",
    default=None,
    help="Path to source file name, relative to source endpoint host path.",
)
@click.option(
	"--destination-file",
    "-df",
    default=None,
    help="Path to destination file name, relative to destination endpoint host path.",
)
@click.option(
    "--label",
    default=DEFAULT_LABEL,
    show_default=True,
	help="Label for the transfer.",
)
@click.option(
	"--verify-checksum",
	"-vc",
	is_flag=True,
    default=True,
    show_default=True,
    help="Verify checksums of files transferred.",
)
@click.option(
	"--batch",
	type=click.File('r'),
    help=textwrap.dedent(
		"""\
        Accept a batch of source/destination file pairs from a file.
		Use '-' to read from stdin.  Uses --source-endpoint and
		--destination-endpoint as passed on the command line.
		
		Uses JSON formatted input.  Example:
        {
            "files": [
                {"source_file": "/data/ds999.9/file1.tar", "destination_file": "/ds999.9/file1.tar"},
	            {"source_file": "/data/ds999.9/file2.tar", "destination_file": "/ds999.9/file2.tar"},
	            {"source_file": "/data/ds999.9/file3.tar", "destination_file": "/ds999.9/file3.tar"}
            ]
        }
"""
    ),
)
@common_options
def transfer_command():
    client = transfer_client()
    
    submit_doc(client, index_id, filename, task_list_file)

    click.echo(
        f"""\
ingest document submission (task submission) complete
task IDs are visible in
    {task_list_file}"""
    )