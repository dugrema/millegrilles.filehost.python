import asyncio
import datetime
import logging
import json
import math
import pathlib
import gzip

from aiohttp import web
from typing import Optional, Union

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie
from millegrilles_messages.messages.Hachage import VerificateurHachage

CONST_CHUNK_SIZE = 64 * 1024        # 64kb
CONST_REFRESH_LISTS = 3_600 * 12    # Every 12 hours


class HostingFileHandler:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

    async def run(self):
        while self.__context.stopping is False:
            await self.maintenance()
            await self.__context.wait(30)

    async def maintenance(self):
        self.__logger.debug("Maintenance cycle")
        await asyncio.gather(self.__manage_file_list_thread())

    async def file_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'fichiers' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        response = web.StreamResponse(status=200)
        await response.prepare(request)

        path_filelist = pathlib.Path(path_idmg, 'list.txt.gz')
        path_filelist_incremental = pathlib.Path(path_idmg, 'list_incremental.txt')

        try:
            with gzip.open(path_filelist, 'r') as fp:
                while True:
                    line = fp.readline(1024)
                    if len(line) == 0:
                        break
                    await response.write(line)
        except FileNotFoundError:
            pass  # No files

        try:
            with open(path_filelist_incremental, 'rt') as fp:
                while True:
                    line = fp.readline(1024)
                    if len(line) == 0:
                        break
                    await response.write(line.encode('utf-8'))
        except FileNotFoundError:
            pass  # No files

        await response.write_eof()
        return response

    async def get_file(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        idmg = cookie.idmg
        fuuid = request.match_info['fuuid']

        # Ensure idmg is already present (authorization should be rejected otherwise)
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            self.__logger.error("Authorized access to non-existant IDMG %s, FAIL" % idmg)
            return web.HTTPForbidden()

        path_fuuid = get_fuuid_dir(path_idmg, fuuid)
        try:
            stat = path_fuuid.stat()
            with open(path_fuuid, 'rb') as fp:
                response = web.StreamResponse(status=200)
                response.content_length = stat.st_size
                response.content_type = 'application/octet-stream'

                await response.prepare(request)
                while True:
                    chunk = fp.read(CONST_CHUNK_SIZE)
                    if len(chunk) == 0:
                        break
                    await response.write(chunk)
                await response.write_eof()
                return response
        except FileNotFoundError:
            return web.HTTPNotFound()

    async def put_file(self, request: web.Request, cookie: Cookie) -> web.Response:
        # This is a read-write function. Ensure proper roles/security level
        if 'fichiers' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        fuuid = request.match_info['fuuid']

        # Ensure idmg is already present (authorization should be rejected otherwise)
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            self.__logger.error("Authorized access to non-existant IDMG %s, FAIL" % idmg)
            return web.HTTPForbidden()

        path_filelist_incremental = pathlib.Path(path_idmg, 'list_incremental.txt')

        try:
            path_fuuid, path_staging = prepare_dir(path_idmg, fuuid)
        except FileExistsError:
            return web.HTTPConflict()
        path_workfile = pathlib.Path(path_staging, fuuid + '.work')

        try:
            # Receive file and move to bucket
            await receive_fuuid(request, path_workfile, fuuid)
            path_workfile.rename(path_fuuid)

            # Add file to incremental list.
            if path_filelist_incremental.exists():
                flag = 'at'  # Append
            else:
                flag = 'wt'  # Create new/overwrite
            with open(path_filelist_incremental, flag) as output:
                output.write(fuuid + '\n')
        finally:
            # Ensure workfile is deleted
            path_workfile.unlink(missing_ok=True)

        return web.HTTPOk()

    async def get_usage(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'fichiers' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        path_usage_file = pathlib.Path(path_idmg, 'usage.json')
        try:
            with open(path_usage_file, 'rt') as fp:
                usage = json.load(fp)
        except FileNotFoundError:
            return web.HTTPNotFound()
        else:
            return web.json_response(usage)

    async def __manage_file_list_thread(self):
        while self.__context.stopping is False:
            files_path = pathlib.Path(self.__context.configuration.dir_files)
            await _manage_file_list(files_path)
            await self.__context.wait(CONST_REFRESH_LISTS)  # Every 12 hours


async def receive_fuuid(request: web.Request, workfile_path: pathlib.Path, fuuid: Optional[str] = None):
    if fuuid:
        verifier = VerificateurHachage(fuuid)
    else:
        verifier = None

    with open(workfile_path, 'wb') as output:
        async for chunk in request.content.iter_chunked(CONST_CHUNK_SIZE):
            output.write(chunk)
            verifier.update(chunk)

    if verifier:
        # Verify digest, raises Exception when mismatch
        verifier.verify()


def get_fuuid_dir(path_idmg: pathlib.Path, fuuid: str) -> pathlib.Path:
    suffix = fuuid[-2:]
    path_bucket = pathlib.Path(path_idmg, 'buckets', suffix)
    path_fuuid = pathlib.Path(path_bucket, fuuid)
    return path_fuuid


def prepare_dir(path_idmg: pathlib.Path, fuuid: str) -> (pathlib.Path, pathlib.Path):
    path_fuuid = get_fuuid_dir(path_idmg, fuuid)
    if path_fuuid.exists():
        # The file already exists
        raise FileExistsError()

    path_staging = pathlib.Path(path_idmg, 'staging')

    # Ensure path bucket and staging exist
    path_staging.mkdir(exist_ok=True)
    path_fuuid.parent.mkdir(parents=True, exist_ok=True)

    return path_fuuid, path_staging


async def iter_bucket_files(path_idmg: pathlib.Path):
    path_buckets = pathlib.Path(path_idmg, 'buckets')

    current_date = datetime.datetime.now()
    fuuid_count = 0
    fuuid_size = 0

    for bucket in path_buckets.iterdir():
        if bucket.is_dir():
            for file in bucket.iterdir():
                if file.is_file():
                    stat = file.stat()
                    fuuid_count += 1
                    fuuid_size += stat.st_size
                    yield file.name

    quota_information = {
        'date': math.floor(current_date.timestamp()),
        'fuuid': {'count': fuuid_count, 'size': fuuid_size}
    }

    yield quota_information


async def _manage_file_list(files_path: pathlib.Path):
    # Creates an updated list of files for each managed idmg
    # Also calculates usage (quotas)
    for idmg_path in files_path.iterdir():
        if idmg_path.is_dir() is False:
            continue  # Skip

        path_usage = pathlib.Path(idmg_path, 'usage.json')
        path_filelist = pathlib.Path(idmg_path, 'list.txt.gz')
        path_filelist_work = pathlib.Path(idmg_path, 'list.txt.gz.work')
        with gzip.open(path_filelist_work, 'wb') as output:
            async for file in iter_bucket_files(idmg_path):
                if isinstance(file, str):
                    filename_bytes = file.encode('utf-8') + b'\n'
                    output.write(filename_bytes)
                elif isinstance(file, dict):
                    # Quota information
                    with open(path_usage, 'wt') as output_usage:
                        json.dump(file, output_usage)

        # Delete old file
        path_filelist.unlink(missing_ok=True)
        # Replace by new file
        path_filelist_work.rename(path_filelist)

        # Remove incremental list
        path_filelist_incremental = pathlib.Path(idmg_path, 'list_incremental.txt')
        path_filelist_incremental.unlink(missing_ok=True)
