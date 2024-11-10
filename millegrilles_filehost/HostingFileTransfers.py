import asyncio
import datetime
import logging
import pathlib

from asyncio import TaskGroup
from typing import Any, Awaitable, Optional, Coroutine, Callable, BinaryIO
from urllib.parse import urljoin

import aiohttp

from millegrilles_filehost.BackupV2 import lire_header_archive_backup
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.HostingBackupFileHandler import HostingBackupFileHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage, Hacheur
from millegrilles_messages.utils.FilePartUploader import UploadState, file_upload_parts

CONST_CHUNK_SIZE = 1024 * 64
CONST_PART_SIZE = 1024 * 1024 * 250


class FileStreamer:

    def __init__(self, fp, filepath: pathlib.Path):
        self.fp = fp
        self.filepath = filepath
        self.state = filepath.stat()
        self.position = 0
        self.size = self.state.st_size

    async def stream(self):
        while True:
            chunk = self.fp.read(CONST_CHUNK_SIZE)
            if len(chunk) == 0:
                break
            self.position += len(chunk)
            # await asyncio.sleep(5)  # Throttle for debug
            yield chunk
        pass


class HostfileFileTransfers:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self._context = context
        self._transfer_queue = asyncio.Queue(maxsize=1)
        self._idmg_event_callback: Optional[Callable[[str, str, dict], Awaitable]] = None

    def set_event_callback(self, callback: Callable[[str, str, dict], Awaitable]):
        self._idmg_event_callback = callback

    async def __clear_queue(self):
        while self._transfer_queue.empty() is False:
            self._transfer_queue.get_nowait()

    async def __stop_thread(self):
        await self._context.wait()
        await self.__clear_queue()
        await self._transfer_queue.put(None)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__stop_thread())
            group.create_task(self._transfer_thread())

    async def add_transfer(self, transfer: dict):
        self._transfer_queue.put_nowait(transfer)

    async def _transfer_thread(self):
        raise NotImplementedError('must override')

    async def _emit_event(self, event_name: str, idmg: str, command_id: str, done: Optional[bool], err: Optional[str] = None, file: Optional[str] = None):
        if err:
            ok = False
        else:
            ok = True
        await self._idmg_event_callback(
            idmg, event_name,
            {'ok': ok, 'err': err, 'done': done, 'command_id': command_id, 'file': file}
        )


class HostfileFileTransfersFuuids(HostfileFileTransfers):
    """
    Transfer process for fuuids
    """

    def __init__(self, context: FileHostContext, hosting_file_handler: HostingFileHandler):
        super().__init__(context)
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__hosting_file_handler = hosting_file_handler

    async def _transfer_thread(self):
        while self._context.stopping is False:
            transfer = await self._transfer_queue.get()
            if transfer is None:
                break  # Exit condition

            try:
                idmg = transfer['idmg']
                content = transfer['content']
                fuuid = content['fuuid']
                command_id = transfer['command']['id']
            except KeyError:
                self.__logger.error("Message missing parameters, skip: %s" % transfer)
                continue

            try:
                self.__logger.debug("Transferring: %s" % transfer)
                await self.__transfer_file(transfer)
            except Exception as e:
                # await self.__idmg_event_callback(
                #     idmg, 'transfer_done',
                #     {'idmg': idmg, 'fuuid': fuuid, 'ok': False, 'err': str(e), 'command_id': command_id}
                # )
                await self._emit_transfer_done(idmg, command_id, fuuid, err=str(e))
                self.__logger.exception("Unhandled transfer exception")

    async def __transfer_file(self, transfer: dict):
        action = transfer['action']
        idmg = transfer['idmg']
        content = transfer['content']
        fuuid = content['fuuid']
        url = content['url']
        filehost_id = content['filehost_id']
        tls_mode = content.get('tls') or 'external'
        command_id = transfer['command']['id']

        self.__logger.debug("__transfer_file (%s) file %s to %s (TLS: %s)" % (action, fuuid, url, tls_mode))
        path_idmg = pathlib.Path(self._context.configuration.dir_files, idmg)

        verify = tls_mode != 'nocheck'
        connector = aiohttp.TCPConnector(verify_ssl=verify)
        async with aiohttp.ClientSession(connector=connector) as session:
            url_authentication = urljoin(url, '/filehost/authenticate')
            async with session.post(url_authentication, json=transfer['command']) as r:
                r.raise_for_status()
            pass

            if action == 'getFile':
                await self.__get_file(session, filehost_id, command_id, url, fuuid, path_idmg)
            elif action == 'putFile':
                await self.__put_file(session, filehost_id, command_id, url, fuuid, path_idmg)

            self.__logger.debug("GET/PUT complete for file %s to %s" % (fuuid, url))
            # await self.__idmg_event_callback(
            #     idmg, 'transfer_done',
            #     {'idmg': idmg, 'fuuid': fuuid, 'ok': True, 'done': True, 'command_id': command_id}
            # )
            await self._emit_transfer_done(idmg, command_id, fuuid)

    async def _emit_transfer_done(self, idmg: str, command_id: str, file: str, err: Optional[str] = None):
        await self._emit_event('transfer_done', idmg, command_id, True, err, file)

    async def __get_file(self, session: aiohttp.ClientSession, filehost_id: str, command_id: str, url: str, fuuid: str, path_idmg: pathlib.Path):
        # Open workfile
        path_work = pathlib.Path(path_idmg, 'staging', fuuid+'.work')
        path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)

        idmg = path_idmg.name

        if path_fuuid.exists():
            # File already transferred. Report as done.
            usage = await self.__hosting_file_handler.get_file_usage(path_idmg)
            await self._idmg_event_callback(
                idmg, 'newFuuid',
                {'file': fuuid, 'usage': usage}
            )

        url_get_file = urljoin(url, f'/filehost/files/{fuuid}')
        verifier = VerificateurHachage(fuuid)

        if path_work.exists():
            raise NotImplementedError('todo - resume download')

        next_update = datetime.datetime.now()

        with open(path_work, 'wb') as fp:
            async with session.get(url_get_file) as r:
                r.raise_for_status()

                transferred = 0
                file_size = r.headers.get('Content-Length')

                await self._idmg_event_callback(
                    idmg, 'transfer_update',
                    {'file': fuuid, 'transferred': 0, 'file_size': file_size, 'filehost_id': filehost_id}
                )

                async for chunk in r.content.iter_chunked(CONST_CHUNK_SIZE):
                    fp.write(chunk)
                    verifier.update(chunk)
                    transferred += len(chunk)

                    if next_update < datetime.datetime.now():
                        await self._idmg_event_callback(
                            idmg, 'transfer_update',
                            {'file': fuuid, 'transferred': transferred, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id}
                        )
                        next_update = datetime.datetime.now() + datetime.timedelta(seconds=5)

        try:
            verifier.verify()  # Raises exception if invalid
        except ErreurHachage as e:
            path_work.unlink()  # Get rid of corrupt work file
            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': fuuid, 'transferred': 0, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id,
                 'done': True, 'err': 'Corrupt file'}
            )
            raise e

        await self._idmg_event_callback(
            idmg, 'transfer_update',
            {'file': fuuid, 'transferred': file_size, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id, 'done': True}
        )

        # Move file
        path_fuuid.parent.mkdir(parents=True, exist_ok=True)
        path_work.rename(path_fuuid)

        # Increment file usage/count
        usage = await self.__hosting_file_handler.update_file_usage(path_fuuid)

        idmg = path_idmg.name
        await self._idmg_event_callback(
            idmg, 'newFuuid',
            {'file': fuuid, 'usage': usage}
        )

    async def __put_file(self, session: aiohttp.ClientSession, filehost_id: str, command_id: str, url: str, fuuid: str, path_idmg: pathlib.Path):
        path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)
        stat_fuuid = path_fuuid.stat()
        file_size = stat_fuuid.st_size

        idmg = path_idmg.name

        with open(path_fuuid, 'rb') as fp:
            url_put_file = urljoin(url, f'/filehost/files/{fuuid}')

            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': fuuid, 'transferred': 0, 'file_size': file_size,
                 'filehost_id': filehost_id, 'command_id': command_id}
            )

            if file_size < CONST_PART_SIZE:
                # One-shot upload
                async with TaskGroup() as group:
                    done_event = asyncio.Event()
                    streamer = FileStreamer(fp, path_fuuid)
                    group.create_task(put_file(session, url_put_file, streamer, done_event))
                    group.create_task(send_update_streamer(self._idmg_event_callback, streamer, filehost_id, command_id, idmg, fuuid, done_event))

                async with session.put(url_put_file, data=fp, headers={'Content-Length': str(file_size)}) as r:
                    if r.status == 409:
                        # File already on server, all done
                        self.__logger.info("__put_file File fuuid:%s already on %s, DONE" % (fuuid, url))
                        return
                    else:
                        r.raise_for_status()
            else:
                # Parts upload
                upload_state = UploadState(fuuid, fp, file_size)
                async with TaskGroup() as group:
                    done_event = asyncio.Event()
                    group.create_task(put_file_parts(session, url_put_file, upload_state, done_event))
                    group.create_task(send_update(self._idmg_event_callback, upload_state, filehost_id, command_id, idmg, fuuid, done_event))

        await self._idmg_event_callback(
            idmg, 'transfer_update',
            {'file': fuuid, 'transferred': file_size, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id, 'done': True}
        )


class HostfileFileTransfersBackup(HostfileFileTransfers):
    """
    Transfer process for backups
    """

    def __init__(self, context: FileHostContext, backup_file_handler: HostingBackupFileHandler):
        super().__init__(context)
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__backup_file_handler = backup_file_handler

    def get_backup_file(self, idmg: str, domain: str, version: str, file: str):
        return self.__backup_file_handler.get_backup_file(idmg, domain, version, file)

    async def _transfer_thread(self):
        while self._context.stopping is False:
            transfer = await self._transfer_queue.get()
            if transfer is None:
                break  # Exit condition

            try:
                idmg = transfer['idmg']
                content = transfer['content']
                file = content['file']
                command_id = transfer['command']['id']
            except KeyError:
                self.__logger.error("Message missing parameters, skip: %s" % transfer)
                continue

            try:
                self.__logger.debug("Transferring: %s" % transfer)
                await self.__transfer_file(transfer)
            except Exception as e:
                self.__logger.exception("Unhandled transfer exception")
                await self._emit_transfer_done(idmg, command_id, file, err=str(e))

    async def __transfer_file(self, transfer: dict):
        action = transfer['action']
        idmg = transfer['idmg']
        content = transfer['content']
        file = content['file']
        version = content['version']
        domain = content['domain']
        url = content['url']
        filehost_id = content['filehost_id']
        tls_mode = content.get('tls') or 'external'
        command_id = transfer['command']['id']

        self.__logger.debug("__transfer_file (%s) file %s to %s (TLS: %s)" % (action, file, url, tls_mode))
        path_idmg = pathlib.Path(self._context.configuration.dir_files, idmg)

        verify = tls_mode != 'nocheck'
        connector = aiohttp.TCPConnector(verify_ssl=verify)
        async with aiohttp.ClientSession(connector=connector) as session:
            url_authentication = urljoin(url, '/filehost/authenticate')
            async with session.post(url_authentication, json=transfer['command']) as r:
                r.raise_for_status()
            pass

            if action == 'getBackupFile':
                await self.__get_backup_file(session, filehost_id, command_id, url, domain, version, file, path_idmg)
            elif action == 'putBackupFile':
                await self.__put_backup_file(session, filehost_id, command_id, url, domain, version, file, path_idmg)

            self.__logger.debug("GET/PUT complete for file %s to %s" % (file, url))
            await self._emit_transfer_done(idmg, command_id, file)

    async def _emit_transfer_done(self, idmg: str, command_id: str, file: str, err: Optional[str] = None):
        await self._emit_event('transfer_done', idmg, command_id, True, err, file)

    async def __get_backup_file(self, session: aiohttp.ClientSession, filehost_id: str, command_id: str, url: str,
                                domain: str, version: str, file: str, path_idmg: pathlib.Path):
        idmg = path_idmg.name

        # Open workfile
        path_work = pathlib.Path(path_idmg, 'staging', file+'.work')
        path_file = self.__backup_file_handler.get_backup_file(idmg, domain, version, file)

        if path_file.exists():
            # File already transferred. Report as done.
            self.__logger.info("Received GET backup file for %s, file already exists" % file)
            return

        # url_get_file = urljoin(url, f'/filehost/files/{fuuid}')
        digester = Hacheur('blake2b-512', 'base58btc')

        if path_work.exists():
            path_work.unlink()  # For now just restart download. Eventually do a resume.

        next_update = datetime.datetime.now()

        with open(path_work, 'wb') as fp:
            async with session.get(url) as r:
                r.raise_for_status()

                transferred = 0
                file_size = r.headers.get('Content-Length')

                await self._idmg_event_callback(
                    idmg, 'transfer_update',
                    {'file': file, 'transferred': 0, 'file_size': file_size, 'filehost_id': filehost_id}
                )

                async for chunk in r.content.iter_chunked(CONST_CHUNK_SIZE):
                    await asyncio.to_thread(fp.write, chunk)
                    digester.update(chunk)
                    transferred += len(chunk)

                    if next_update < datetime.datetime.now():
                        await self._idmg_event_callback(
                            idmg, 'transfer_update',
                            {'file': file, 'transferred': transferred, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id}
                        )
                        next_update = datetime.datetime.now() + datetime.timedelta(seconds=5)

        digest_value = digester.finalize()  # Raises exception if invalid
        # Check that digest matches end of the file
        suffix_digest_file = file.split('.')[0].split('_').pop()
        if digest_value.endswith(suffix_digest_file) is False:
            # Get rid of corrupt work file
            path_work.unlink()  # Delete file
            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': file, 'transferred': 0, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id,
                 'done': True, 'err': 'Corrupt file'}
            )
            raise Exception('Downloaded backup file %s is corrupt' % file)

        with open(path_work, 'rb') as backup_file:
            # Lire le header du fichier de backup
            header_archive = await asyncio.to_thread(lire_header_archive_backup, backup_file)
        if header_archive['domaine'] != domain:
            # Wrong domain, reject file
            path_work.unlink()  # Delete file
            raise Exception('Downloaded backup file header has wrong domain, reject %s' % file)

        char_type_archive = header_archive['type_archive']

        # File seems good, ensure folder exists
        path_file.parent.mkdir(parents=True, exist_ok=True)

        if char_type_archive == 'C':
            # Create info.json/courant.json if this is a C file
            path_domain = path_file.parent.parent
            await self.__backup_file_handler.create_info_files(path_domain, header_archive, version)

        # Move file
        path_work.rename(path_file)

        await self._idmg_event_callback(
            idmg, 'transfer_update',
            {'file': file, 'transferred': file_size, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id, 'done': True}
        )


    async def __put_backup_file(self, session: aiohttp.ClientSession, filehost_id: str, command_id: str, url: str,
                                domain: str, version: str, file: str, path_idmg: pathlib.Path):
        idmg = path_idmg.name

        # Open workfile
        path_work = pathlib.Path(path_idmg, 'staging', file+'.work')
        path_file = self.__backup_file_handler.get_backup_file(idmg, domain, version, file)

        if path_file.exists() is False:
            # Unknown file, cannnot PUT.
            self.__logger.error("Received PUT backup file for unknown file %s, abort" % file)
            await self._idmg_event_callback(
                idmg, 'transfer_done',
                {'file': file, 'ok': False, 'err': 'Unknown file', 'filehost_id': filehost_id, 'command_id': command_id, 'done': True}
            )
            return

        stat_fuuid = path_file.stat()
        file_size = stat_fuuid.st_size

        with open(path_file, 'rb') as fp:
            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': file, 'transferred': 0, 'file_size': file_size,
                 'filehost_id': filehost_id, 'command_id': command_id}
            )

            # One-shot upload
            async with TaskGroup() as group:
                done_event = asyncio.Event()
                streamer = FileStreamer(fp, path_file)
                group.create_task(put_file(session, url, streamer, done_event))
                group.create_task(send_update_streamer(self._idmg_event_callback, streamer, filehost_id, command_id, idmg, file, done_event))

        await self._idmg_event_callback(
            idmg, 'transfer_done',
            {'file': file, 'transferred': file_size, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id, 'done': True}
        )


async def put_file(session: aiohttp.ClientSession, url: str, streamer: FileStreamer, done_event: asyncio.Event):
    try:
        async with session.put(url, data=streamer.stream(), headers={'Content-Length': str(streamer.size)}) as r:
            if r.status == 409:  # File already on server - OK
                pass
            else:
                r.raise_for_status()
    finally:
        done_event.set()


async def send_update_streamer(idmg_event_callback, streamer: FileStreamer, filehost_id: str, command_id: str,
                               idmg: str, file: str, done_event: asyncio.Event):
    while done_event.is_set() is False:
        await idmg_event_callback(
            idmg, 'transfer_update',
            {'file': file, 'transferred': streamer.position, 'file_size': streamer.size,
             'filehost_id': filehost_id, 'command_id': command_id}
        )
        try:
            await asyncio.wait_for(done_event.wait(), 5)
        except asyncio.TimeoutError:
            pass


async def put_file_parts(session: aiohttp.ClientSession, url_put_file: str,
                         upload_state: UploadState, done_event: asyncio.Event):
    try:
        await file_upload_parts(session, url_put_file, upload_state, batch_size=CONST_PART_SIZE)
    finally:
        done_event.set()


async def send_update(idmg_event_callback, upload_state: UploadState, filehost_id: str, command_id: str,
                      idmg: str, fuuid: str, done_event: asyncio.Event):
    while done_event.is_set() is False:
        await idmg_event_callback(
            idmg, 'transfer_update',
            {'fuuid': fuuid, 'transferred': upload_state.position, 'file_size': upload_state.size,
             'filehost_id': filehost_id, 'command_id': command_id}
        )
        try:
            await asyncio.wait_for(done_event.wait(), 5)
        except asyncio.TimeoutError:
            pass
