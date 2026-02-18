#!/usr/bin/env python3

"""
Script to manage transfers of data backup tar files from GDEX Lustre storage to the TACC Globus endpoint.
"""
import os
from pathlib import Path
import sys
from rda_python_globus.lib import transfer_client
from rda_python_globus.lib.config import ENDPOINT_ALIASES, TACC_BASE_PATH
from rda_python_common.PgDBI import pgget, pgadd, pgupdt
from globus_sdk import TransferData, GlobusAPIError
import logging

my_logger = logging.getLogger(__name__)

TACC_LUSTRE_BASE_PATH = "/lustre/desc1/gdex/work/tacc_backups"
LOGPATH = os.path.join(TACC_LUSTRE_BASE_PATH, 'logs', 'tacc_transfer.log')

lustre_endpoint = ENDPOINT_ALIASES.get("gdex-lustre")
tacc_endpoint = ENDPOINT_ALIASES.get("tacc")

MAX_ACTIVE_TASKS = 4
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024 * 1024  # 10 TB

def get_task(task_id: str, namespace: str) -> dict:
    """ Get details about a Globus task. """
    tc = transfer_client(namespace=namespace)
    try:
        task = tc.get_task(task_id)
        return task
    except GlobusAPIError as e:
        msg = ("Globus API Error\n"
               "HTTP status: {}\n"
               "Error code: {}\n"
               "Error message: {}").format(e.http_status, e.code, e.message)
        my_logger.error(msg)
        raise e

def get_tasks(namespace: str, filters: dict) -> list:
    """ Get list of Globus tasks with optional filtering. """
    tc = transfer_client(namespace=namespace)
    tasks = []
    for task in tc.paginated.task_list(**filters).items():
        tasks.append(task)
    return tasks

def submit_transfer_task(
        source_endpoint: str, 
        destination_endpoint: str, 
        source_path: str, 
        destination_path: str, 
        label: str, 
        namespace: str,
        verify_checksum: bool = True
        ) -> dict:
    """ Submit a Globus transfer task for a single file. """
    tc = transfer_client(namespace=namespace)
    transfer_data = TransferData(
        transfer_client=tc,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        label=label,
        verify_checksum=verify_checksum,
        notify_on_succeeded=False
    )
    transfer_data.add_item(source_path, destination_path)

    try:
        task = tc.submit_transfer(transfer_data)
        return task
    except GlobusAPIError as e:
        msg = ("Globus API Error\n"
               "HTTP status: {}\n"
               "Error code: {}\n"
               "Error message: {}").format(e.http_status, e.code, e.message)
        my_logger.error(msg)
        raise e

def check_tar_files():
    """ Check for tar files in the tacc_backups directory and update transfer status record in the database. """
    # Get list of tar files in the 'tacc_backups' directory
    tar_files = list(Path(TACC_LUSTRE_BASE_PATH).glob("*fn*.tar"))

    # Check if tar files have an entry in the 'tacc_backups' table with a Globus task ID
    for tar_file in tar_files:
        my_logger.info(f"Checking for task associated with {tar_file}...")
        file = os.path.basename(str(tar_file))
        tar_record = pgget(
            "tacc_backups",
            "file, task_id, status",
            f"file='{file}'"
        )
        if tar_record and tar_record["task_id"]:
            my_logger.info(f"Found record for {file}: {tar_record}")
            
            # Check status of associated Globus task and update record if status has changed
            task_id = tar_record["task_id"]
            try:
                task_info = get_task(task_id, namespace="tacc")
                if task_info['status'] != tar_record["status"]:
                    record = {
                        "status": task_info['status']                
                        }
                    if task_info['status'] not in ["ACTIVE", "INACTIVE"]:
                        record["completion_time"] = task_info['completion_time']
                    pgupdt(
                        "tacc_backups",
                        record,
                        f"file='{file}'"
                    )
                    my_logger.info(f"Updated status for {file} to {task_info['status']}.")
                else:
                    my_logger.info(f"Status for {file} is still {task_info['status']}. No update needed.")
            except GlobusAPIError:
                msg = f"Failed to get task info for {file} with task ID {task_id}."
                my_logger.warning(msg)
        else:
            my_logger.info(f"No record found for {file}.")
    
    return

def move_completed_files():
    """ Move tar files with completed transfers to the 'completed' directory. """
    # Get list of tar files in the 'tacc_backups' directory
    tar_files = list(Path(TACC_LUSTRE_BASE_PATH).glob("*fn*.tar"))

    for tar_file in tar_files:
        file = os.path.basename(str(tar_file))
        tar_record = pgget(
            "tacc_backups",
            "file, status",
            f"file='{file}'"
        )
        if tar_record and tar_record["status"] == "SUCCEEDED":
            my_logger.info(f"Transfer for {file} succeeded. Moving tar file to the 'completed' directory.")
            completed_dir = os.path.join(TACC_LUSTRE_BASE_PATH, "completed")
            os.makedirs(completed_dir, exist_ok=True)
            destination_path = os.path.join(completed_dir, file)
            try:
                os.rename(tar_file, destination_path)
                my_logger.info(f"Moved {file} to {destination_path}.")
            except OSError as e:
                my_logger.error(f"Error moving file {file} to {destination_path}: {e}")

    return

def submit_new_transfers():
    """
    Submit new transfer tasks for tar files in the 'tacc_backups' directory.  
    Only submit tasks for files that do not already have an associated Globus 
    task ID in the database and are smaller than the maximum allowed file size. 
    Also check the number of active tasks before submitting new transfers to 
    avoid exceeding the maximum allowed active tasks.
    """
    # Get list of tar files in the 'tacc_backups' directory
    tar_files = list(Path(TACC_LUSTRE_BASE_PATH).glob("*fn*.tar"))

    # Check if tar files have an entry in the 'tacc_backups' table with a Globus task ID
    for tar_file in tar_files:

        # Skip files larger than MAX_FILE_SIZE_BYTES and log a warning
        file_size = tar_file.stat().st_size
        if file_size > MAX_FILE_SIZE_BYTES:
            my_logger.warning(f"File {tar_file} is larger than the maximum allowed size of {MAX_FILE_SIZE_BYTES} bytes. Skipping transfer for this file.")
            continue
    
        my_logger.info(f"Checking for task associated with {tar_file}...")
        file = os.path.basename(str(tar_file))
        tar_record = pgget(
            "tacc_backups",
            "file, task_id, status",
            f"file='{file}'"
        )
        if tar_record and tar_record["task_id"]:
            my_logger.info(f"Found record for {file}: {tar_record}. Skipping submission of new transfer task since there is already a record with a Globus task ID. Will check status of existing task and update record in the database in the next run of the script.")
        else:
            active_tasks = pgget(
                "tacc_backups",
                "count(*)",
                "status='ACTIVE'"
            )["count"]
            if active_tasks >= MAX_ACTIVE_TASKS:
                my_logger.warning(f"Maximum number of active tasks ({MAX_ACTIVE_TASKS}) reached. Skipping submission for {file} until other tasks complete.")
                return

            my_logger.info(f"No record found for {file}. Submitting new transfer task.")
            # Submit transfer task to Globus
        
            source_path = os.path.join("work/tacc_backups", file)
            destination_path = os.path.join(TACC_BASE_PATH, "gdex-data-backups", file)

            transfer_result = submit_transfer_task(
                source_endpoint=lustre_endpoint,
                destination_endpoint=tacc_endpoint,
                source_path=source_path,
                destination_path=destination_path,
                label=f"Transfer {file}",
                namespace="tacc",
                verify_checksum=False
            )

            if transfer_result['code'] == "Accepted":
                my_logger.info(f"{transfer_result['message']} for file {file}\nTask ID: {transfer_result['task_id']}")
                # Get task info from Globus API
                task_info = get_task(transfer_result["task_id"], namespace="tacc")

                task_record = {
                    "task_id": task_info['task_id'], 
                    "status": task_info['status'], 
                    "request_time" : task_info['request_time'],
                    "source_endpoint": task_info['source_endpoint_id'], 
                    "destination_endpoint": task_info['destination_endpoint_id'],
                    "source_endpoint_display_name": task_info['source_endpoint_display_name'], 
                    "destination_endpoint_display_name": task_info['destination_endpoint_display_name'],
                    "file": file
                }
                pgadd("tacc_backups", task_record)
                my_logger.info(f"Added record for {file} to tacc_backups table.")
            else:
                my_logger.error(f"Failed to submit transfer task for {file}. Response: {transfer_result}")

    return

def configure_log(**kwargs):
    """ Set up logging configuration """

    if 'loglevel' in kwargs:
        loglevel = kwargs['loglevel']
    else:
        loglevel = 'info'

    level = getattr(logging, loglevel.upper())
    my_logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    """ Rotating file handler """
    rfh = logging.handlers.RotatingFileHandler(LOGPATH,maxBytes=200000000,backupCount=10)
    rfh.setLevel(level)
    rfh.setFormatter(formatter)
    my_logger.addHandler(rfh)

    """ stdout handler """
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)
    my_logger.addHandler(stdout_handler)
		
    return

#----------------------------------------------------------------------------------------

configure_log(loglevel='info')

# First check status of existing tasks and update records in the database before 
# submitting new transfer tasks. This ensures that we have the most up-to-date 
# information about active tasks and available capacity before submitting new transfers.
check_tar_files()

# Next, move any tar files with completed transfers to the 'completed' directory 
# before submitting new transfer tasks. This helps keep the tacc_backups directory 
# organized and prevents confusion about which files have completed transfers.
move_completed_files()

# Check how many active Globus tasks are currently processing.  Exit if there are 
# already MAX_ACTIVE_TASKS active tasks.
active_tasks = get_tasks(namespace="tacc", filters={"filter": "status:ACTIVE"})
if len(active_tasks) >= MAX_ACTIVE_TASKS:
    my_logger.warning(f"Maximum number of active tasks ({MAX_ACTIVE_TASKS}) reached. Exiting without submitting new transfer tasks until other tasks complete.")
else:
    # Finally, submit new transfer tasks for tar files in the 'tacc_backups' directory 
    # that do not already have an associated Globus task ID in the database and are 
    # smaller than the maximum allowed file size.
    submit_new_transfers()
