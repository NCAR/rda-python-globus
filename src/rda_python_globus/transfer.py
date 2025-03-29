import json
import os
import typing as t
import textwrap

import click
from globus_sdk import TransferData, GlobusAPIError, NetworkError

from .lib import (
    common_options, 
	task_submission_options,
    transfer_client,
	process_json_stream,
	TRANSFER_SOURCE,
    TRANSFER_DESTINATION,
)

import logging
logger = logging.getLogger(__name__)

DEFAULT_LABEL = "RDA Quasar transfer"

def add_batch_to_transfer_data(batch):
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

def submit_transfer(data):
	""" General data transfer to RDA endpoints.  Input should be JSON formatted input 
	    if transferring multiple files in batch mode. """

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
		Use '-' to read from stdin and close the stream with 'Ctrl + D'.  
		Uses --source-endpoint and --destination-endpoint as passed 
		on the command line.
		
		Uses JSON formatted input.  Example:
		$ dsglobus transfer --source-endpoint SOURCE_ENDPOINT --destination-endpoint DESTINATION_ENDPOINT --batch -
        {
            "files": [
                {"source_file": "/data/ds999.9/file1.tar", "destination_file": "/ds999.9/file1.tar"},
	            {"source_file": "/data/ds999.9/file2.tar", "destination_file": "/ds999.9/file2.tar"},
	            {"source_file": "/data/ds999.9/file3.tar", "destination_file": "/ds999.9/file3.tar"}
            ]
        }
		<Ctrl+D>
"""
    ),
)
@common_options
@task_submission_options
def transfer_command(
	source_endpoint: str,
	destination_endpoint: str,
	source_file: str,
	destination_file: str,
	verify_checksum: bool,
	batch: t.TextIO | None,
	dry_run: bool,
	label: str | None,
	) -> None:
	"""Submit a Globus transfer task."""
	
	tc = transfer_client()
		
	transfer_data = TransferData(transfer_client=tc,
							     source_endpoint=source_endpoint,
							     destination_endpoint=destination_endpoint,
							     label=DEFAULT_LABEL,
							     verify_checksum=verify_checksum)

	if batch:
		transfer_data = add_batch_to_transfer_data(batch)
	else:
		transfer_data.add_item(source_file, destination_file)
		
	if dry_run:
		data = transfer_data.data
		msg = "Source endpoint: {0}".format(data['source_endpoint'])
		msg += "\nDestination endpoint: {0}".format(data['destination_endpoint'])
		msg += "\nLabel: {0}".format(data['label'])
		msg += "\nVerify checksum: {0}".format(data['verify_checksum'])
		msg += "\nTransfer items:\n{0}".format(json.dumps(data['DATA'], indent=2))

        # exit safely
		return
    
	res = transfer_client.submit_transfer(transfer_data)
	task_id = res["task_id"]
	msg = "{0}\nTask ID: {1}".format(transfer_result['message'], task_id)
	logger.info(msg)
	
	click.echo(f"""{msg}""")