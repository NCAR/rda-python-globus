import json
import os
import logging
import re
import typing as t
import six

import click

from .auth import token_storage_adapter, auth_client, transfer_client
from .config import TRANSFER_SOURCE, TRANSFER_DESTINATION

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
        help="Don't actually submit the task, print submission data instead as a sanity check.",
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

def print_table(iterable, headers_and_keys, print_headers=True):
    """ 
    Print an iterable in table format.  
    The iterable may not be safe to walk multiple times, so we must walk it
    only once -- however, to let us write things naturally, convert it to a
    list and we can assume it is safe to walk repeatedly
    """

    iterable = list(iterable)

    # extract headers and keys as separate lists
    headers = [h for (h, k) in headers_and_keys]
    keys = [k for (h, k) in headers_and_keys]

    # convert all keys to keyfuncs
    keyfuncs = [_key_to_keyfunc(key) for key in keys]

    # use the iterable to find the max width of an element for each column, in
    # the same order as the headers_and_keys array
    # use a special function to handle empty iterable
    def get_max_colwidth(kf):
        def _safelen(x):
            try:
                return len(x)
            except TypeError:
                return len(str(x))
        lengths = [_safelen(kf(i)) for i in iterable]
        if not lengths:
            return 0
        else:
             return max(lengths)

    widths = [get_max_colwidth(kf) for kf in keyfuncs]

    # handle the case in which the column header is the widest thing
    widths = [max(w, len(h)) for w, h in zip(widths, headers)]

    # create a format string based on column widths
    format_str = " | ".join("{:" + str(w) + "}" for w in widths)

    def none_to_null(val):
        if val is None:
            return "NULL"
        return val

    # print headers
    if print_headers:
        print(format_str.format(*[h for h in headers]))
        print(format_str.format(*["-" * w for w in widths]))
    # print the rows of data
    for i in iterable:
        print(format_str.format(*[none_to_null(kf(i)) for kf in keyfuncs]))

    return

def _key_to_keyfunc(k):
    """
    We allow for 'keys' which are functions that map columns onto value
    types -- they may do formatting or inspect multiple values on the
    object. In order to support this, wrap string keys in a simple function
    that does the natural lookup operation, but return any functions we
    receive as they are.
    """
    # if the key is a string, then the "keyfunc" is just a basic lookup
    # operation -- return that
    if isinstance(k, six.string_types):
        def lookup(x):
            return x[k]

        return lookup
    # otherwise, the key must be a function which is executed on the item
    # to produce a value -- return it verbatim
    return k

def configure_log():
   """ Congigure logging """
   logfile = os.path.join(LOGPATH, 'dsglobus-app.log')
   loglevel = 'INFO'
   format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
   logging.basicConfig(filename=logfile, level=loglevel, format=format)

   return

class CustomEpilog(click.Group):
    def format_epilog(self, ctx, formatter):
        if self.epilog:
            formatter.write_paragraph()
            for line in self.epilog.split('\n'):
                formatter.write_text(line)

__all__ = (
    "common_options",
    "task_submission_options",
    "validate_dsid",
    "prettyprint_json",
    "process_json_stream",
    "print_table",
    "configure_log",
    "token_storage_adapter",
    "auth_client",
    "transfer_client",
    "TRANSFER_SOURCE",
    "TRANSFER_DESTINATION",
    "CustomEpilog",
)
