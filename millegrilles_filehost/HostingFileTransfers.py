import asyncio
import datetime
import logging
import pathlib

from asyncio import TaskGroup
from typing import Awaitable, Optional, Callable
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


class GetTransferStatus:

    def __init__(self, filehost_id: str, command_id: str, idmg: str, fuuid: str, url_file: str):
        self.filehost_id = filehost_id
        self.command_id = command_id
        self.idmg = idmg
        self.fuuid = fuuid
        self.url_file = url_file

        self.stop_event = asyncio.Event()
        self.position = 0
        self.file_size: Optional[int] = None


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
        try:
            while self._context.stopping is False:
                transfer = await self._transfer_queue.get()
                if transfer is None:
                    break  # Exit condition

                try:
                    idmg = transfer['idmg']
                    content = transfer['content']
                    fuuid = content['fuuid']
                    url = content['url']
                    command_id = transfer['command']['id']
                except KeyError:
                    self.__logger.error("Message missing parameters, skip: %s" % transfer)
                    continue

                try:
                    self.__logger.debug("Transferring: %s" % transfer)
                    await self.__transfer_file(transfer)
                except asyncio.CancelledError:
                    break  # Stopping
                except aiohttp.ClientConnectorError as e:
                    self.__logger.exception("Connection error for file %s to %s" % (fuuid, url))
                    await self._emit_transfer_done(idmg, command_id, fuuid, err=str(e))
                except Exception as e:
                    self.__logger.exception(f'Unhandled transfer exception on transfer of {fuuid} with {url}')
                    # await self.__idmg_event_callback(
                    #     idmg, 'transfer_done',
                    #     {'idmg': idmg, 'file': fuuid, 'ok': False, 'err': str(e), 'command_id': command_id}
                    # )
                    try:
                        await self._emit_transfer_done(idmg, command_id, fuuid, err=str(e))
                    except Exception as e:
                        self.__logger.warning("_emit_transfer_done Error: %s" % str(e))
        except:
            self.__logger.exception("_transfer_thread Fatal exception, stopping")
        finally:
            if self._context.stopping is False:
                self.__logger.error("_transfer_thread stopping without context being stopped")
                self._context.stop()

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
            else:
                raise ValueError('Unsupported action: %s' % action)

            self.__logger.debug("GET/PUT complete for file %s to %s" % (fuuid, url))
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
            await self._idmg_event_callback(idmg, 'newFuuid', {'file': fuuid, 'usage': usage})

        url_get_file = urljoin(url, f'/filehost/files/{fuuid}')
        get_status = GetTransferStatus(filehost_id, command_id, idmg, fuuid, url_get_file)
        timeout_error = None
        attempt = 0
        attempt_reset = datetime.datetime.now()
        while attempt < 4:
            attempt += 1
            try:
                if path_work.exists():
                    verifier = await self.__get_file_resume(path_work, session, get_status)
                else:
                    verifier = await self.__get_file_fromstart(path_work, session, get_status)
                break  # Transfer successful
            except* asyncio.TimeoutError as e:
                timeout_error = e
                if datetime.datetime.now() - datetime.timedelta(minutes=5) > attempt_reset:
                    # Reset attempts after 5 minutes of initial try
                    attempt_reset = datetime.datetime.now()
                    attempt = 0
                    self.__logger.warning(f"Timeout on file transfer - resetting attempts for fuuid {fuuid}")
                else:
                    self.__logger.warning(f"Timeout on file transfer attempt {attempt} for fuuid {fuuid}")
                await asyncio.sleep(3)  # Waiting 3 seconds before next attempt

        else:
            if timeout_error:
                raise timeout_error
            raise Exception('File transfer error - timeout during transfer')

        try:
            verifier.verify()  # Raises exception if invalid
        except ErreurHachage as e:
            path_work.unlink()  # Get rid of corrupt work file
            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': fuuid, 'filehost_id': filehost_id, 'command_id': command_id, 'done': True, 'err': 'Corrupt file'}
            )
            raise e

        file_size = path_work.stat().st_size
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
        await self._idmg_event_callback(idmg, 'newFuuid', {'file': fuuid, 'usage': usage})

    async def __get_file_fromstart(self, path_work: pathlib.Path, session: aiohttp.ClientSession, get_status: GetTransferStatus) -> VerificateurHachage:
        # Ensure work dirs exist
        path_work.parent.mkdir(parents=True, exist_ok=True)
        verifier = VerificateurHachage(get_status.fuuid)
        with open(path_work, 'wb') as fp:
            async with session.get(get_status.url_file) as r:
                r.raise_for_status()

                file_size = int(r.headers.get('Content-Length'))
                get_status.file_size = file_size
                await self.__getting_file(get_status, fp, r, verifier)

        return verifier


    async def __getting_file(self, get_status: GetTransferStatus, fp,
                             response: aiohttp.ClientResponse, verifier: Optional[VerificateurHachage] = None):
        # Create a download task and a task to update the file controler regularly to avoid a timeout
        async with TaskGroup() as group:
            group.create_task(self.__getting_file_update_events(get_status))
            group.create_task(self.__getting_file_download(get_status, fp, response, verifier))

    async def __getting_file_update_events(self, get_status: GetTransferStatus):
        await self._idmg_event_callback(
            get_status.idmg, 'transfer_update',
            {'file': get_status.fuuid, 'transferred': get_status.position, 'file_size': get_status.file_size, 'filehost_id': get_status.filehost_id}
        )

        while get_status.stop_event.is_set() is False:
            await self._idmg_event_callback(
                get_status.idmg, 'transfer_update',
                {'file': get_status.fuuid, 'transferred': get_status.position, 'file_size': get_status.file_size,
                 'filehost_id': get_status.filehost_id, 'command_id':get_status.command_id}
            )
            try:
                await asyncio.wait_for(get_status.stop_event.wait(), 5)
                return  # Done
            except asyncio.TimeoutError:
                pass  # Loop

    async def __getting_file_download(self, get_status: GetTransferStatus, fp, response: aiohttp.ClientResponse,
                                      verifier: Optional[VerificateurHachage] = None):

        try:
            async for chunk in response.content.iter_chunked(CONST_CHUNK_SIZE):
                fp.write(chunk)
                if verifier:
                    verifier.update(chunk)
                get_status.position += len(chunk)
        finally:
            get_status.stop_event.set()  # Release the update_events task

    async def __get_file_resume(self, path_work: pathlib.Path, session: aiohttp.ClientSession, get_status: GetTransferStatus) -> VerificateurHachage:

        stat_file = path_work.stat()
        start_position = stat_file.st_size
        fp = None
        try:
            headers = {'Range': 'bytes=%d-' % start_position}

            async with session.get(get_status.url_file, headers=headers) as response:
                response.raise_for_status()
                file_size = int(response.headers.get('Content-Length'))
                get_status.file_size = file_size
                if response.status == 206:
                    # Resuming
                    fp = open(path_work, 'ab')
                    verifier = None
                    get_status.position = start_position   # Resume at position
                elif response.status == 200:
                    # Can't resume, restarting file
                    self.__logger.warning("Failed to resume file %s, restarting GET from start" % get_status.fuuid)
                    fp = open(path_work, 'wb')
                    verifier = VerificateurHachage(get_status.fuuid)
                else:
                    raise Exception('Wrong status code: %d' % response.status)

                await self.__getting_file(get_status, fp, response, verifier)

        finally:
            if fp:
                fp.close()

        if verifier is None:
            # Verify file content from start
            verifier = VerificateurHachage(get_status.fuuid)
            with open(path_work, 'rb') as fp:
                while True:
                    chunk = await asyncio.to_thread(fp.read, CONST_CHUNK_SIZE)
                    if len(chunk) == 0:
                        break
                    verifier.update(chunk)
            return verifier
        else:
            return verifier

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
            except asyncio.CancelledError:
                break  # Stopping
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
            else:
                raise ValueError('Unsupported action type: %s' % action)

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

        if path_work.exists():
            # path_work.unlink()  # For now just restart download. Eventually do a resume.
            stat_file = path_work.stat()
            start_position = stat_file.st_size
            headers = {'Range': 'bytes=%d-' % start_position}
        else:
            start_position = 0
            headers = None

        next_update = datetime.datetime.now()

        # Ensure work path exists
        path_work.parent.mkdir(parents=True, exist_ok=True)
        digester = None
        fp = None

        try:
            position = 0
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                file_size = int(response.headers.get('Content-Length'))
                if response.status == 206:
                    # Resuming
                    fp = open(path_work, 'ab')
                    position = start_position   # Resume at position
                elif response.status == 200:
                    # Can't resume, restarting file
                    self.__logger.warning("Failed to resume backup %s, restarting GET from start" % url)
                    fp = open(path_work, 'wb')
                    digester = Hacheur('blake2b-512', 'base58btc')
                else:
                    raise Exception('Wrong status code: %d' % response.status)

                await self._idmg_event_callback(
                    idmg, 'transfer_update',
                    {'file': file, 'transferred': position, 'file_size': file_size, 'filehost_id': filehost_id}
                )

                async for chunk in response.content.iter_chunked(CONST_CHUNK_SIZE):
                    await asyncio.to_thread(fp.write, chunk)
                    if digester:
                        digester.update(chunk)
                    position += len(chunk)

                    if next_update < datetime.datetime.now():
                        await self._idmg_event_callback(
                            idmg, 'transfer_update',
                            {'file': file, 'transferred': position, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id}
                        )
                        next_update = datetime.datetime.now() + datetime.timedelta(seconds=5)

        finally:
            if fp:
                fp.close()

        if digester is None:
            # File resumed, digest from start
            digester = Hacheur('blake2b-512', 'base58btc')
            with open(path_work, 'rb') as fp:
                with open(path_work, 'rb') as fp:
                    while True:
                        chunk = await asyncio.to_thread(fp.read, CONST_CHUNK_SIZE)
                        if len(chunk) == 0:
                            break
                        digester.update(chunk)

        try:
            digest_value = digester.finalize()  # Raises exception if invalid
        except Exception as e:
            path_work.unlink()  # Something is wrong, delete file
            raise e

        # Check that digest matches end of the file
        suffix_digest_file = file.split('.')[0].split('_').pop()
        if digest_value.endswith(suffix_digest_file) is False:
            # Get rid of corrupt work file
            path_work.unlink()  # Delete file
            await self._idmg_event_callback(
                idmg, 'transfer_update',
                {'file': file, 'transferred': 0, 'file_size': file_size, 'filehost_id': filehost_id, 'command_id': command_id,
                 'done': True, 'err': 'Corrupt file - digest mismatch'}
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

        # Ensure file already exists
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
            {'file': fuuid, 'transferred': upload_state.position, 'file_size': upload_state.size,
             'filehost_id': filehost_id, 'command_id': command_id}
        )
        try:
            await asyncio.wait_for(done_event.wait(), 5)
        except asyncio.TimeoutError:
            pass

