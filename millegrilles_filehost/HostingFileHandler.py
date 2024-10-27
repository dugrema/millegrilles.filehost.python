import logging
import pathlib

from aiohttp import web
from typing import Optional, Union

from gridfs.errors import FileExists

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie
from millegrilles_messages.messages.Hachage import VerificateurHachage


CONST_CHUNK_SIZE = 64 * 1024

# Path:

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

        async for filename in iter_bucket_files(path_idmg):
            filename_bytes = filename.encode('utf-8') + b'\n'
            await response.write(filename_bytes)

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

        try:
            path_fuuid, path_staging = prepare_dir(path_idmg, fuuid)
        except FileExists:
            return web.HTTPConflict()
        path_workfile = pathlib.Path(path_staging, fuuid + '.work')

        try:
            # Receive file and move to bucket
            await receive_fuuid(request, path_workfile, fuuid)
            path_workfile.rename(path_fuuid)
        finally:
            # Ensure workfile is deleted
            path_workfile.unlink(missing_ok=True)

        return web.HTTPOk()


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
        raise FileExists()

    path_staging = pathlib.Path(path_idmg, 'staging')

    # Ensure path bucket and staging exist
    path_staging.mkdir(exist_ok=True)
    path_fuuid.parent.mkdir(parents=True, exist_ok=True)

    return path_fuuid, path_staging


async def iter_bucket_files(path_idmg: pathlib.Path):
    path_buckets = pathlib.Path(path_idmg, 'buckets')
    for bucket in path_buckets.iterdir():
        if bucket.is_dir():
            for file in bucket.iterdir():
                if file.is_file():
                    yield file.name
