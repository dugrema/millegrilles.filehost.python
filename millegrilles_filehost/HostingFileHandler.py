import asyncio
import datetime
import logging
import json
import math
import pathlib
import gzip
import re

from aiohttp import web
from typing import Optional, Union

from shutil import rmtree

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.utils.FilePartUploader import CHUNK_SIZE

CONST_CHUNK_SIZE = 64 * 1024                # 64kb
CONST_REFRESH_LISTS_INTERVAl = 3_600 * 8    # Every 8 hours
CONST_MAINTAIN_STAGING_INTERVAL = 3_600 * 1 # Every hour

LOGGER = logging.getLogger(__name__)

class HostingFileEventListener:

    def __init__(self):
        pass

    async def on_event(self, idmg: str, name: str, value: dict):
        pass


class HostingFileHandler:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__event_listeners: list[HostingFileEventListener] = list()
        self.__semaphore_usage_update = context.semaphore_usage_update

    async def run(self):
        await self.maintenance()

    def add_event_listener(self, listener: HostingFileEventListener):
        self.__event_listeners.append(listener)

    async def emit_event(self, idmg: str, name: str, value: dict):
        for l in self.__event_listeners:
            await l.on_event(idmg, name, value)

    async def maintenance(self):
        self.__logger.debug("Starting maintenance")
        done, pending = await asyncio.wait([
            asyncio.create_task(self.__manage_file_list_thread()),
            asyncio.create_task(self.__manage_staging_thread()),
            # asyncio.create_task(self.__emit_status_thread()),
        ], return_when=asyncio.FIRST_COMPLETED)
        if self.__context.stopping is False:
            self.__logger.error("Maintenant thread stopped out of turn")
            self.__context.stop()
        await asyncio.gather(*pending, return_exceptions=True)
        self.__logger.debug("Maintenance stopped")

    async def file_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'filecontroler' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        response = web.StreamResponse(status=200)
        response.enable_compression()  # Compress the output if applicable
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
                    line = await asyncio.to_thread(fp.readline, 1024)
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
            return await stream_reponse(request, path_fuuid)
        except FileNotFoundError:
            return web.HTTPNotFound()

    async def put_file(self, request: web.Request, cookie: Cookie) -> web.Response:
        # This is a read-write function. Ensure proper roles/security level
        if 'filecontroler' in cookie.get('roles'):
            pass
        elif 'usager' in cookie.get('roles') and cookie.get('user_id') is not None:
            pass
        else:
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
            path_fuuid, path_staging = await asyncio.to_thread(prepare_dir, path_idmg, fuuid)
        except FileExistsError:
            return web.HTTPConflict()
        path_workfile = pathlib.Path(path_staging, fuuid + '.work')

        try:
            # Receive file and move to bucket
            await receive_fuuid(request, path_workfile, fuuid)
            await asyncio.to_thread(path_workfile.rename, path_fuuid)

            # Add file to incremental list.
            if path_filelist_incremental.exists():
                flag = 'at'  # Append
            else:
                flag = 'wt'  # Create new/overwrite
            with open(path_filelist_incremental, flag) as output:
                await asyncio.to_thread(output.write, fuuid + '\n')
        finally:
            # Ensure workfile is deleted
            await asyncio.to_thread(path_workfile.unlink, missing_ok=True)

        # Increment filecount/file size
        try:
            usage = await update_file_usage(path_idmg, path_fuuid, self.__semaphore_usage_update)
            await self.emit_event(idmg, 'newFuuid', {'fuuid': fuuid, 'usage': usage})
        except:
            self.__logger.exception("Error udpating file usage information")

        return web.HTTPOk()

    async def delete_file(self, request: web.Request, cookie: Cookie) -> web.Response:
        # This is a read-write function. Ensure proper roles/security level
        if 'filecontroler' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        fuuid = request.match_info['fuuid']

        # Ensure idmg is already present (authorization should be rejected otherwise)
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            self.__logger.error("Authorized access to non-existant IDMG %s, FAIL" % idmg)
            return web.HTTPForbidden()

        path_fuuid = get_fuuid_dir(path_idmg, fuuid)

        try:
            await asyncio.to_thread(path_fuuid.unlink)
        except FileNotFoundError:
            return web.HTTPNotFound()

        return web.HTTPOk()

    async def get_usage(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'filecontroler' not in cookie.get('roles'):
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        try:
            usage = await get_file_usage(path_idmg, self.__semaphore_usage_update)
        except FileNotFoundError:
            return web.HTTPNotFound()
        else:
            return web.json_response(usage)

    async def __manage_file_list_thread(self):
        while self.__context.stopping is False:
            files_path = pathlib.Path(self.__context.configuration.dir_files)
            await _manage_file_list(files_path, self.__semaphore_usage_update, self.emit_event)
            await self.__context.wait(CONST_REFRESH_LISTS_INTERVAl)

    async def __manage_staging_thread(self):
        while self.__context.stopping is False:
            files_path = pathlib.Path(self.__context.configuration.dir_files)
            await _manage_staging(files_path)
            await self.__context.wait(CONST_MAINTAIN_STAGING_INTERVAL)  # Every 12 hours

    # async def __emit_status_thread(self):
    #     while self.__context.stopping is False:
    #         # Go through all idmg folders, load usage.json and emit on socket.io
    #         path_files = pathlib.Path(self.__context.configuration.dir_files)
    #         for f in path_files.iterdir():
    #             if f.is_dir():
    #                 idmg = f.name
    #                 path_usage = pathlib.Path(f, 'usage.json')
    #                 try:
    #                     async with self.__semaphore_usage_update:
    #                         with open(path_usage, 'rt+') as fp:
    #                             usage = await asyncio.to_thread(json.load, fp)
    #                     await self.emit_event(idmg, 'usage', usage)
    #                 except FileNotFoundError:
    #                     pass
    #                 except json.JSONDecodeError as e:
    #                     self.__logger.warning("Error opening usage.json for %s: %s" % (idmg, e))
    #         await self.__context.wait(3600 * 12)

    async def put_file_part(self, request: web.Request, cookie: Cookie) -> web.Response:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'filecontroler' in cookie.get('roles'):
            pass
        elif 'usager' in cookie.get('roles') and cookie.get('user_id') is not None:
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        # Check if file exists
        fuuid = request.match_info['fuuid']
        position = request.match_info['position']
        path_fuuid = get_fuuid_dir(path_idmg, fuuid)
        if path_fuuid.exists():
            return web.HTTPConflict()  # 409 - File already exists

        # Create staging area
        path_staging = pathlib.Path(path_idmg, 'staging', fuuid)
        path_staging.mkdir(parents=True, exist_ok=True)
        path_part = pathlib.Path(path_staging, str(position) + '.part')
        if path_part.exists():
            return web.HTTPPreconditionFailed()  # 412 - Indicates the part is already uploaded and valid

        path_part_work = pathlib.Path(path_staging, str(position) + '.work')
        if path_part_work.exists():
            return web.HTTPExpectationFailed()  # 417 - Upload already in progress

        try:
            with open(path_part_work, 'wb') as output:
                async for chunk in request.content.iter_chunked(CONST_CHUNK_SIZE):
                    await asyncio.to_thread(output.write, chunk)
                path_part_work.rename(path_part)
        finally:
            path_part_work.unlink(missing_ok=True)  # Ensure cleanup

        return web.HTTPOk()

    async def finish_file(self, request: web.Request, cookie: Cookie) -> web.Response:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if 'filecontroler' in cookie.get('roles'):
            pass
        elif 'usager' in cookie.get('roles') and cookie.get('user_id') is not None:
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        # Check if file exists
        fuuid = request.match_info['fuuid']
        try:
            path_fuuid, path_staging = await asyncio.to_thread(prepare_dir, path_idmg, fuuid)
        except FileExistsError:
            return web.HTTPConflict()

        # Check if parts directory exists
        path_fuuid_staging = pathlib.Path(path_idmg, 'staging', fuuid)
        if path_fuuid_staging.exists() is False:
            return web.HTTPNotFound()  # Staging folder does not exist/is gone

        path_workfile = pathlib.Path(path_staging, fuuid + '.work')
        verifier = VerificateurHachage(fuuid)

        try:
            with open(path_workfile, 'wb') as output:
                for part in file_part_reader(path_fuuid_staging):
                    with open(part, 'rb') as input_file:
                        while True:
                            chunk = await asyncio.to_thread(input_file.read, CHUNK_SIZE)
                            if len(chunk) == 0:
                                break
                            await asyncio.to_thread(output.write, chunk)
                            verifier.update(chunk)

            verifier.verify()  # Raises exception on error

            # File is good
            path_workfile.rename(path_fuuid)

            # Cleanup staging
            await asyncio.to_thread(rmtree, path_fuuid_staging, ignore_errors=True)
        except ErreurHachage:
            # Cleanup fuuid staging, files are bad
            await asyncio.to_thread(rmtree, path_fuuid_staging, ignore_errors=True)
            return web.HTTPFailedDependency()  # Staged content bad, has to be re-uploaded.
        finally:
            path_workfile.unlink(missing_ok=True)

        # Increment filecount/file size
        try:
            usage = await update_file_usage(path_idmg, path_fuuid, self.__semaphore_usage_update)
            await self.emit_event(idmg, 'newFuuid', {'fuuid': fuuid, 'usage': usage})
        except:
            self.__logger.exception("Error udpating file usage information")

        return web.HTTPOk()


async def receive_fuuid(request: web.Request, workfile_path: pathlib.Path, fuuid: Optional[str] = None):
    if fuuid:
        verifier = VerificateurHachage(fuuid)
    else:
        verifier = None

    with open(workfile_path, 'wb') as output:
        async for chunk in request.content.iter_chunked(CONST_CHUNK_SIZE):
            await asyncio.to_thread(output.write, chunk)
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
    """
    Generator that returns file names (dict: {name: str}) for all file buckets. The last item is a dict of stats.
    :param path_idmg:
    :return:
    """
    path_buckets = pathlib.Path(path_idmg, 'buckets')

    current_date = datetime.datetime.now()
    fuuid_count = 0
    fuuid_size = 0

    try:
        for bucket in path_buckets.iterdir():
            if bucket.is_dir():
                for file in bucket.iterdir():
                    if file.is_file():
                        stat = file.stat()
                        fuuid_count += 1
                        fuuid_size += stat.st_size
                        yield {'name': file.name}
    except FileNotFoundError:
        pass  # No buckets

    quota_information = {
        'date': math.floor(current_date.timestamp()),
        'fuuid': {'count': fuuid_count, 'size': fuuid_size}
    }

    yield quota_information


async def _manage_file_list(files_path: pathlib.Path, semaphore: asyncio.Semaphore, emit_event):
    # Creates an updated list of files for each managed idmg
    # Also calculates usage (quotas)
    for idmg_path in files_path.iterdir():
        idmg = idmg_path.name
        if idmg_path.is_dir() is False:
            continue  # Skip

        path_usage = pathlib.Path(idmg_path, 'usage.json')
        path_filelist = pathlib.Path(idmg_path, 'list.txt.gz')
        path_filelist_work = pathlib.Path(idmg_path, 'list.txt.gz.work')
        with gzip.open(path_filelist_work, 'wb') as output:
            async for bucket_info in iter_bucket_files(idmg_path):
                try:
                    filename: str = bucket_info['name']
                    filename_bytes = filename.encode('utf-8') + b'\n'
                    await asyncio.to_thread(output.write, filename_bytes)
                except KeyError:
                    # Quota information
                    async with semaphore:
                        with open(path_usage, 'wt') as output_usage:
                            await asyncio.to_thread(json.dump, bucket_info, output_usage)
                            try:
                                await emit_event(idmg, 'usage', bucket_info)
                            except Exception as e:
                                LOGGER.warning("Error emitting usage event: %s" % e)

        # Delete old file
        path_filelist.unlink(missing_ok=True)
        # Replace by new file
        path_filelist_work.rename(path_filelist)

        # Remove incremental list
        path_filelist_incremental = pathlib.Path(idmg_path, 'list_incremental.txt')
        path_filelist_incremental.unlink(missing_ok=True)


def file_part_reader(path_parts: pathlib.Path):
    parts = list()
    for part in path_parts.iterdir():
        if part.is_file() and part.name.endswith('.part'):
            position = int(part.name.split('.')[0])
            parts.append({'part': part, 'position': position})

    # Sort parts by position
    parts = sorted(parts, key=lambda x: x['position'])

    for part in parts:
        yield part['part']


async def _manage_staging(files_path: pathlib.Path):

    now = datetime.datetime.now()
    expired = now - datetime.timedelta(hours=2)
    expired_epoch = expired.timestamp()

    for idmg_path in files_path.iterdir():
        staging_path = pathlib.Path(idmg_path, 'staging')
        if staging_path.exists() is False:
            continue  # No staging, skip

        for item in staging_path.iterdir():
            stat = await asyncio.to_thread(item.stat)
            if stat.st_mtime > expired_epoch:
                continue  # Not expired

            # Expired item, delete it
            LOGGER.info("Removing stale staging item %s" % item)
            if item.is_file():
                await asyncio.to_thread(item.unlink)
            elif item.is_dir():
                await asyncio.to_thread(rmtree, item, ignore_errors=True)
            else:
                LOGGER.warning("Unhandled stale item type: %s" % item)


async def get_file_usage(path_idmg: pathlib.Path, semaphore: asyncio.Semaphore):
    if path_idmg.exists() is False:
        return web.HTTPForbidden()  # IDMG is not hosted

    path_usage_file = pathlib.Path(path_idmg, 'usage.json')
    async with semaphore:
        with open(path_usage_file, 'rt') as fp:
            usage = json.load(fp)

    return usage


async def update_file_usage(path_idmg: pathlib.Path, path_fuuid: pathlib.Path, semaphore: asyncio.Semaphore) -> dict:
    # Increment filecount/file size
    stat = path_fuuid.stat()
    file_size = stat.st_size
    path_usage_file = pathlib.Path(path_idmg, 'usage.json')
    now = math.floor(datetime.datetime.now().timestamp())
    async with semaphore:
        try:
            with open(path_usage_file, 'r+') as fp:
                usage_file = await asyncio.to_thread(json.load, fp)
                fuuid = usage_file['fuuid']
                fuuid['count'] = fuuid['count'] + 1
                fuuid['size'] = fuuid['size'] + file_size
                usage_file['date'] = now
                fp.seek(0)
                await asyncio.to_thread(json.dump, usage_file, fp)
                fp.truncate()
        except FileNotFoundError:
            with open(path_usage_file, 'wt') as fp:
                usage_file = {'date': now, 'fuuid': {'count': 1, 'size': file_size}}
                await asyncio.to_thread(json.dump, usage_file, fp)

    return usage_file


async def stream_reponse(request: web.Request, filepath: pathlib.Path,
                         size_limit: Optional[int] = None) -> Union[web.Response, web.StreamResponse]:
    method = request.method
    fuuid = request.match_info['fuuid']
    headers = request.headers

    range_bytes = headers.get('Range')

    etag = fuuid[-16:]  # ETag requis pour caching, utiliser 16 derniers caracteres du fuuid

    stat_fichier = filepath.stat()  # Throws FileNotFoundError

    taille_fichier = stat_fichier.st_size
    if size_limit is not None and taille_fichier > size_limit:
        LOGGER.error(f"stream_reponse Taille fichier {fuuid} depasse limite {size_limit}")
        return web.HTTPExpectationFailed()

    range_str = None

    headers_response = {
        'Cache-Control': 'public, max-age=604800, immutable',
        'Accept-Ranges': 'bytes',
    }

    if range_bytes is not None:
        # Calculer le content range, taille transfert
        range_parsed = parse_range(range_bytes, taille_fichier)
        start = range_parsed['start']
        end = range_parsed['end']
        taille_transfert = str(end - start + 1)
        range_str = f'bytes {start}-{end}/{taille_fichier}'
        headers_response['Content-Range'] = range_str
    else:
        start = None
        end = None
        # Transferer tout le contenu
        if taille_fichier:
            taille_transfert = str(taille_fichier)
        else:
            taille_transfert = None

    if range_str is not None:
        status = 206
    else:
        status = 200

    # Preparer reponse, headers
    response = web.StreamResponse(status=status, headers=headers_response)
    response.content_length = taille_transfert
    response.content_type = 'application/stream'
    response.etag = etag

    LOGGER.info("stream_reponse Stream fichier %s : Content-Length : %s, Content-Range: %s" % (fuuid, taille_transfert, range_str))

    await response.prepare(request)
    if method == 'HEAD':
        await response.write_eof()
        return response

    try:
        with open(filepath, 'rb') as fp:
            if start is not None and start > 0:
                fp.seek(start, 0)
                position = start
            else:
                position = 0

            while True:
                chunk = fp.read(64*1024)
                if not chunk:
                    break

                if end is not None and position + len(chunk) > end:
                    taille_chunk = end - position + 1
                    await response.write(chunk[:taille_chunk])
                    break  # Termine
                else:
                    await response.write(chunk)

                position += len(chunk)
    finally:
        await response.write_eof()

    return response


def parse_range(range, taille_totale):
    re_compiled = re.compile('bytes=([0-9]*)\\-([0-9]*)?')
    m = re_compiled.search(range)

    start = m.group(1)
    if start is not None:
        start = int(start)
    else:
        start = 0

    end = m.group(2)
    if end is None:
        end = taille_totale - 1
    else:
        try:
            end = int(end)
            if end > taille_totale:
                end = taille_totale - 1
        except ValueError:
            end = taille_totale - 1

    result = {
        'start': start,
        'end': end,
    }

    return result
