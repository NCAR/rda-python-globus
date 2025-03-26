import json
import os
import logging
from glob import glob
from io import StringIO
import re

import click

from .auth import auth_client, internal_auth_client
from .search import search_client

RDA_BASE_PATH = '/glade/campaign/collections/rda'
LOGPATH = os.path.join(RDA_BASE_PATH, 'work/tcram/logs/globus')

def common_options(f):
    # any shared/common options for all commands
    return click.help_option("-h", "--help")(f)

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

def configure_log():
   """ Congigure logging """
   logfile = os.path.join(LOGPATH, 'dataset-search.log')
   loglevel = 'INFO'
   format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
   logging.basicConfig(filename=logfile, level=loglevel, format=format)

   return

__all__ = (
    "common_options",
    "validate_dsid",
    "prettyprint_json",
    "configure_log",
    "config_storage_adapter",
)
