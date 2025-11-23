import time
import datetime
import gzip
import logging
import multiprocessing
import pathlib
import signal
import sys

from os import nice

from typing import Optional

from millegrilles_filehost.Configuration import FileHostConfiguration
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

CONST_CHUNK_SIZE = 1024 * 64

LOGGER = logging.getLogger(__name__)

STOPPING = False


def exit_gracefully(signum=None, frame=None):
    sys.exit(3)  # Forced exit


def check_files_process(configuration: FileHostConfiguration, idmg_path: pathlib.Path, not_after_date: datetime.datetime, q: multiprocessing.queues.Queue):
    # Setup signals
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    # Increment niceness by 15
    new_priority = nice(15)
    LOGGER.debug("Starting to check files (priority: %d)" % new_priority)
    try:
        result = check_files_idmg(configuration, idmg_path, not_after_date)
        q.put(result)
    except:
        q.put(False)
    LOGGER.debug("Done checking files")
    time.sleep(15)


def check_files_idmg(configuration: FileHostConfiguration, idmg_path: pathlib.Path, not_after_date: datetime.datetime) -> bool:
    files_checked = 0
    bytes_checked = 0

    buckets_path = pathlib.Path(idmg_path, 'buckets')
    if buckets_path.exists() is False:
        return True  # No files yet

    # Prepare the dumpster in case we find corrupted files
    dumpster_path = pathlib.Path(idmg_path, 'dumpster')
    dumpster_path.mkdir(exist_ok=True)
    corrupt_log_path = pathlib.Path(idmg_path, 'corrupt.txt')

    file_count_limit = configuration.check_batch_len
    file_bytes_limit = configuration.check_batch_size
    check_throttle_ms = configuration.check_throttle_ms
    complete: Optional[bool] = None

    idmg = idmg_path.name
    check_start = datetime.datetime.now()

    path_filecheck = pathlib.Path(idmg_path, 'check_listing.txt.gz')
    path_filecheck_position = pathlib.Path(idmg_path, 'check_position.txt')

    # Load initial file position in listing to skip all files already checked
    try:
        with open(path_filecheck_position, 'rt') as fp:
            initial_file_position = int(fp.read())
    except (FileNotFoundError, ValueError):
        initial_file_position = 0

    file_position = 0  # Position of current file in filecheck listing

    with gzip.open(path_filecheck, 'rt') as fp_filecheck:
        complete: Optional[bool] = None
        for file_path_str in fp_filecheck:
            # Skip files up to current counter position
            if initial_file_position > file_position:
                file_position += 1
                continue  # Skip file

            file_path_str = file_path_str.strip()
            LOGGER.debug(f"check_files_idmg Check file {file_path_str}")
            file = pathlib.Path(file_path_str)
            fuuid = file.name
            try:
                stats = file.stat()
            except FileNotFoundError:
                file_position += 1
                LOGGER.info(f"check_files_idmg File IDMG:{idmg} FUUID:{fuuid} is gone (deleted?)")
                continue

            file_ok = verify_hosted_file(file, check_throttle_ms)
            if not file_ok:
                LOGGER.warning(f"check_files_idmg File IDMG:{idmg} FUUID:{fuuid} content is corrupt, moving file to dumpster")

                # Move the corrupt file to the dumpster
                path_dumped_file = pathlib.Path(dumpster_path, file.name)
                file.rename(path_dumped_file)

                # Add an entry to corrupt.txt
                with open(corrupt_log_path, 'at') as fp:
                    fp.write(file.name)
                    fp.write('\n')
            else:
                LOGGER.debug(f"check_files_idmg File IDMG:{idmg} FUUID:{fuuid} is OK")

            file_position += 1
            files_checked += 1
            bytes_checked += stats.st_size

            if files_checked >= file_count_limit or bytes_checked >= file_bytes_limit:
                complete = False
                break  # Batch limit reached
        else:
            complete = True

    # Save current position in list
    with open(path_filecheck_position, 'wt') as fp:
        fp.write(str(file_position))

    # # Use list of completed buckets, allows skipping
    # buckets_completed_path = pathlib.Path(idmg_path, 'buckets_completed.txt')
    # buckets_completed = set()
    # try:
    #     with open(buckets_completed_path, 'rt') as fp:
    #         for line in fp:
    #             buckets_completed.add(line.strip())
    # except FileNotFoundError:
    #     pass
    #
    # for bucket in buckets_path.iterdir():
    #
    #     if bucket.name in buckets_completed:
    #         continue    # Bucket already completely processed
    #
    #     file: pathlib.Path
    #     for file in bucket.iterdir():
    #         if not file.is_file():
    #             continue  # Only checking files
    #
    #         fuuid = file.name
    #
    #         stats = file.stat()
    #         last_modified = stats.st_mtime
    #         if last_modified > not_after_date_ts:
    #             continue  # Skip file, is was modified since the start of this batch
    #         elif last_modified > ts_days_check:
    #             continue  # The file could be included in this batch but it was modified (checked) < N days ago. Skipping.
    #
    #         file_ok = verify_hosted_file(file, check_throttle_ms)
    #         if file_ok is False:
    #             LOGGER.warning(f"File IDMG:{idmg} FUUID:{fuuid} content is corrupt, moving file to dumpster")
    #
    #             # Move the corrupt file to the dumpster
    #             path_dumped_file = pathlib.Path(dumpster_path, file.name)
    #             file.rename(path_dumped_file)
    #
    #             # Add an entry to corrupt.txt
    #             with open(corrupt_log_path, 'at') as fp:
    #                 fp.write(file.name)
    #                 fp.write('\n')
    #
    #         else:
    #             LOGGER.debug(f"File IDMG:{idmg} FUUID:{fuuid} is OK")
    #
    #         files_checked += 1
    #         bytes_checked += stats.st_size
    #
    #         if files_checked >= file_count_limit:
    #             complete = False
    #             break  # Batch limit reached
    #         elif bytes_checked >= file_bytes_limit:
    #             complete = False
    #
    #         # Inner loop
    #         if complete is not None:
    #             break  # Batch limit reached
    #
    #     # Outer loop
    #     if complete is not None:
    #         break  # Batch limit reached
    #
    #     # Keep track of completed buckets
    #     buckets_completed.add(bucket.name)
    #     with open(buckets_completed_path, 'at') as fp:
    #         fp.write(bucket.name)
    #         fp.write('\n')
    # else:
    #     complete = True

    check_end = datetime.datetime.now()
    check_duration = check_end - check_start
    if files_checked > 0:
        LOGGER.info(f"File checking on IDMG:%s has completed a batch in %s on %s files (%s bytes)",
                         idmg, check_duration, files_checked, bytes_checked)

    # if complete:
    #     buckets_completed_path.unlink(missing_ok=True)  # Remove buckets completed file

    if complete is None:
        return False

    return complete


def verify_hosted_file(file_path: pathlib.Path, throttle: Optional[int]) -> bool:
    global STOPPING

    fuuid = file_path.name
    verifier = VerificateurHachage(fuuid)

    if throttle is None or throttle == 0:
        throttle = None
    else:
       throttle = float(throttle) / 1000

    with open(file_path, 'rb') as fp:
        while True:
            chunk = fp.read(CONST_CHUNK_SIZE)
            if not chunk:
                break

            verifier.update(chunk)
            if throttle:  # Throttle file check
                time.sleep(throttle)

            if STOPPING:
                raise Exception('stopping')

    try:
        verifier.verify()
        file_path.touch(exist_ok=True)  # Modify file time, avoids re-checking in same batch
        return True
    except ErreurHachage:
        return False
