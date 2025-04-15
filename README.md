# dsglobus

This application is a command-line tool for Globus data transfer and management of files 
archived in the NSF NCAR Research Data Archive.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to 
install `rda_python_globus`.

From within your Python virtual environment:
```
pip install rda-python-globus
```

After installation, the cli command `dsglobus` will be available in
the /bin directory of your virtual environment.

## Command-line usage

The `dsglobus` app is run with the following subcommands.  Each supports a
`--help/-h` option for details and examples on its usage:
```
dsglobus transfer --help
dsglobus get-task --help
dsglobus task-list --help
dsglobus cancel-task --help
dsglobus ls --help
dsglobus mkdir --help
dsglobus rename --help
dsglobus delete --help
```

### Example usage
1. Transfer a single file from the `NCAR RDA GLADE` endpoint to the `NCAR RDA Quasar`
endpoint:
```
$ dsglobus transfer \
    --source-endpoint rda-glade \
    --destination-endpoint rda-quasar \
    --source-file /data/d999009/file.txt \
    --destination-file /d999009/file.txt
```
2. Multiple files can be transferred with a single `dsglobus transfer` call by 
passing a JSON formatted list of files.  To transfer a batch of files from a JSON file:
```
$ dsglobus transfer \
    --source-endpoint SOURCE_ENDPOINT \
    --destination-endpoint DESTINATION_ENDPOINT \
    --batch /path/to/batch.json
```
where the contents of `batch.json` is formatted with `source_file/destination_file`
pairs as:
```
{
    "files": [
        {"source_file": "/data/d999009/file1.tar", "destination_file": "/d999009/file1.tar"},
        {"source_file": "/data/d999009/file2.tar", "destination_file": "/d999009/file2.tar"},
        {"source_file": "/data/d999009/file3.tar", "destination_file": "/d999009/file3.tar"}
    ]
}
```

### Listing contents of a directory on a Globus endpoint

A listing of files on a Globus endpoint can be retrieved via the `dsglobus ls` command.  This
command supports filtering the results subject to the following rules:

- Filter patterns must start with `--, ~, !`, or `!~`.  If none of these are given, `=` will 
be used
- `=` does exact matching
- `~` does regex matching, supporting globs (`*`)
- `!` does inverse `=` matching
- `!~` does inverse `~` matching
- `~*.txt` matches all `.txt` files, for example

Examples:
```
$ dsglobus ls -ep <endpoint> -p <path> --filter '~*.txt'       # all txt files
$ dsglobus ls -ep <endpoint> -p <path> --filter '!~file1.*'    # not starting in "file1."
$ dsglobus ls -ep <endpoint> -p <path> --filter '~*ile3.tx*'   # anything with "ile3.tx"
$ dsglobus ls -ep <endpoint> -p <path> --filter '=file2.txt'   # only "file2.txt"
$ dsglobus ls -ep <endpoint> -p <path> --filter 'file2.txt'    # same as '=file2.txt'
$ dsglobus ls -ep <endpoint> -p <path> --filter '!=file2.txt'  # anything but "file2.txt"
```

## Customizing and extending dsglobus

This app can be modified and adapted to be used on other Globus clients and endpoints with
minimal effort.  Simply update the client ID, token storage, endpoint IDs, endpoint aliases, and 
other configuration parameters in `rda_globus_python/lib/config.py` to adapt the app to your use 
case and specific needs.

## Resources

This app is adapted from the fully featured [Globus Command Line Interface (CLI)](https://docs.globus.org/cli/) 
and uses the 
[`TransferClient` class from the Globus SDK](https://globus-sdk-python.readthedocs.io/en/stable/services/transfer.html).

The full [Globus Transfer documentation](https://docs.globus.org/api/transfer/) offers full
details about the service and reference documentation for all of
its supported methods and features.