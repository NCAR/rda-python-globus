import json
import os
import logging
import re
import typing as t

import click

from .auth import token_storage_adapter, auth_client, transfer_client

RDA_BASE_PATH = '/glade/campaign/collections/rda'
LOGPATH = os.path.join(RDA_BASE_PATH, 'work/tcram/logs/globus')

def common_options(f):
    # any shared/common options for all commands
    return click.help_option("-h", "--help")(f)

def task_submission_options(f):
    """ Options shared by both transfer and delete task submission. """

    f = click.option(
        "--dry-run",
        is_flag=True,
        help="Don't actually submit the task, print submission data instead",
    )(f)
    f = click.option("--label", "-l", default="", help="Label for the task")(f)

    return f

def validate_dsid(ctx, param, dsid):
    """ Validate dsid from command line input """
    ms = re.match(r'^([a-z]{1})(\d{3})(\d{3})$', dsid)
    if ms:
        return dsid
    else:
        raise click.BadParameter("format must be 'dnnnnnn'")

def prettyprint_json(obj, fp=None):
    if fp:
        return json.dump(obj, fp, indent=2, separators=(",", ": "), ensure_ascii=False)
    return json.dumps(obj, indent=2, separators=(",", ": "), ensure_ascii=False)

def process_json_stream(stream: t.TextIO) -> t.Dict[str, t.Any]:
    """ Process a JSON stream from stdin and return a JSON object. """
    
    json_str = ""

    # Read the stream line by line
    for lineno, line in enumerate(stream.readlines()):
        # Skip empty lines
        if not line.strip():
            continue
        json_str += line
    
    obj = json.loads(json_str)
    return obj

def configure_log():
   """ Congigure logging """
   logfile = os.path.join(LOGPATH, 'dsglobus-app.log')
   loglevel = 'INFO'
   format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
   logging.basicConfig(filename=logfile, level=loglevel, format=format)

   return

__all__ = (
    "common_options",
    "task_submission_options",
    "validate_dsid",
    "prettyprint_json",
    "process_json_stream",
    "configure_log",
    "token_storage_adapter",
    "auth_client",
    "transfer_client",
)
