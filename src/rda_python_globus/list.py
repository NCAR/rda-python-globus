import json
import typing as t
import textwrap

import click
from globus_sdk import TransferData, GlobusAPIError, NetworkError

from .lib import (
    common_options,
    print_table, 
    transfer_client,
	RDA_QUASAR_ENDPOINT,
)

import logging
logger = logging.getLogger(__name__)

@click.command(
    "ls",
    short_help="List files on an endpoint",
)
@click.option(
    "--endpoint",
    "-ep",
    type=str,
	default=RDA_QUASAR_ENDPOINT,
    show_default=True,
    help="Endpoint ID or name.",
)
@click.option(
    "--path",
    "-p",
    type=str,
    help="Directory path on endpoint. If not given, will use the root directory of the endpoint.",
)
@click.option(
    "--filter",
    "-f",
    type=str,
    help="Filter pattern for file listing. See help for details.",
)
@common_options
def ls_command(
    endpoint: str,
	path: str,
    filter: str
) -> None:
    """ List endpoint directory contents 
	
	=== Filtering
	List files and dirs on a specific path on an endpoint, filtering in various ways.

    Filter patterns must start with "=", "~", "!", or "!~"
    If none of these are given, "=" will be used

    "=" does exact matching
    "~" does regex matching, supporting globs (*)
    "!" does inverse "=" matching
    "!~" does inverse "~" matching

    "~*.txt" matches all .txt files, for example
    
	$ dsglobus ls -ep <endpoint> -p <path> --filter '~*.txt'  # all txt files
	$ dsglobus ls -ep <endpoint> -p <path> --filter '!~file1.*'  # not starting in "file1."
	$ dsglobus ls -ep <endpoint> -p <path> --filter '~*ile3.tx*'  # anything with "ile3.tx"
	$ dsglobus ls -ep <endpoint> -p <path> --filter '=file2.txt'  # only "file2.txt"
	$ dsglobus ls -ep <endpoint> -p <path> --filter 'file2.txt'  # same as '=file2.txt'
	$ dsglobus ls -ep <endpoint> -p <path> --filter '!=file2.txt'  # anything but "file2.txt"
    """

    ls_params = {}

    if path:
        ls_params.update{"path": path}
    if filter:
        ls_params.update({"filter": "name:{}".format(filter)})	

    def cleaned_item_name(item):
        return item["name"] + ("/" if item["type"] == "dir" else "")
        
    fields=[
			("User", "user"),
			("Group", "group"),
			("Permissions", "permissions"),
			("Size", "size"),
			("Last Modified", "last_modified"),
			("File Type", "type"),
			("Filename", cleaned_item_name),
	]

    tc = transfer_client()
    ls_response = tc.operation_ls(endpoint, **ls_params)
    print_table(ls_response, fields)
	