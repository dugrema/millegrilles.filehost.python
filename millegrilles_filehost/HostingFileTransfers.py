import asyncio
import logging
import pathlib

from asyncio import TaskGroup
from typing import Any, Awaitable, Optional, Coroutine, Callable, BinaryIO
from urllib.parse import urljoin

import aiohttp

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.HostingFileHandler import HostingFileHandler
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.utils.FilePartUploader import UploadState, file_upload_parts

CONST_CHUNK_SIZE = 1024 * 64


class HostfileFileTransfers:

    def __init__(self, context: FileHostContext, hosting_file_handler: HostingFileHandler):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__hosting_file_handler = hosting_file_handler
        self.__transfer_queue = asyncio.Queue(maxsize=1)
        self.__idmg_event_callback: Optional[Callable[[str, str, dict], Awaitable]] = None

    def set_event_callback(self, callback: Callable[[str, str, dict], Awaitable]):
        self.__idmg_event_callback = callback

    async def __clear_queue(self):
        while self.__transfer_queue.empty() is False:
            self.__transfer_queue.get_nowait()

    async def __stop_thread(self):
        await self.__context.wait()
        await self.__clear_queue()
        await self.__transfer_queue.put(None)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__stop_thread())
            group.create_task(self.__transfer_thread())

    async def add_transfer(self, transfer: dict):
        self.__transfer_queue.put_nowait(transfer)

    async def __transfer_thread(self):
        while self.__context.stopping is False:
            transfer = await self.__transfer_queue.get()
            if transfer is None:
                break  # Exit condition

            try:
                idmg = transfer['idmg']
                content = transfer['content']
                fuuid = content['fuuid']
                message_id = transfer['command']['id']
            except KeyError:
                self.__logger.error("Message missing parameters, skip: %s" % transfer)
                continue

            try:
                self.__logger.debug("Transferring: %s" % transfer)
                await self.__transfer_file(transfer)
            except Exception as e:
                await self.__idmg_event_callback(
                    idmg, 'transfer_done',
                    {'idmg': idmg, 'fuuid': fuuid, 'ok': False, 'err': str(e), 'command_id': message_id}
                )
                self.__logger.exception("Unhandled transfer exception")

    async def __transfer_file(self, transfer: dict):
        action = transfer['action']
        idmg = transfer['idmg']
        content = transfer['content']
        fuuid = content['fuuid']
        url = content['url']
        tls_mode = content.get('tls') or 'external'
        message_id = transfer['command']['id']

        self.__logger.debug("PUT file %s to %s (TLS: %s)" % (fuuid, url, tls_mode))
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)

        verify = tls_mode != 'nocheck'
        connector = aiohttp.TCPConnector(verify_ssl=verify)
        async with aiohttp.ClientSession(connector=connector) as session:
            url_authentication = urljoin(url, '/filehost/authenticate')
            async with session.post(url_authentication, json=transfer['command']) as r:
                r.raise_for_status()
            pass

            if action == 'getFile':
                await self.__get_file(session, url, fuuid, path_idmg)
            elif action == 'putFile':
                await self.__put_file(session, url, fuuid, path_idmg)

            self.__logger.debug("PUT complete for file %s to %s" % (fuuid, url))
            await self.__idmg_event_callback(
                idmg, 'transfer_done',
                {'idmg': idmg, 'fuuid': fuuid, 'ok': True, 'done': True, 'command_id': message_id}
            )

    async def __get_file(self, session: aiohttp.ClientSession, url: str, fuuid: str, path_idmg: pathlib.Path):
        # Open workfile
        path_work = pathlib.Path(path_idmg, 'staging', fuuid+'.work')
        path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)

        idmg = path_idmg.name

        if path_fuuid.exists():
            # File already transferred. Report as done.
            usage = await self.__hosting_file_handler.get_file_usage(path_idmg)
            await self.__idmg_event_callback(
                idmg, 'newFuuid',
                {'fuuid': fuuid, 'usage': usage}
            )

        url_get_file = urljoin(url, f'/filehost/files/{fuuid}')
        verifier = VerificateurHachage(fuuid)

        if path_work.exists():
            raise NotImplementedError('todo - resume download')

        with open(path_work, 'wb') as fp:
            async with session.get(url_get_file) as r:
                r.raise_for_status()

                async for chunk in r.content.iter_chunked(CONST_CHUNK_SIZE):
                    fp.write(chunk)
                    verifier.update(chunk)

        try:
            verifier.verify()  # Raises exception if invalid
        except ErreurHachage as e:
            path_work.unlink()  # Get rid of corrupt work file
            raise e

        # Move file
        path_fuuid.parent.mkdir(parents=True, exist_ok=True)
        path_work.rename(path_fuuid)

        idmg = path_idmg.name
        usage = await self.__hosting_file_handler.get_file_usage(path_idmg)
        await self.__idmg_event_callback(
            idmg, 'newFuuid',
            {'fuuid': fuuid, 'usage': usage}
        )

    async def __put_file(self, session: aiohttp.ClientSession, url: str, fuuid: str, path_idmg: pathlib.Path):
        path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)
        stat_fuuid = path_fuuid.stat()
        file_size = stat_fuuid.st_size

        with open(path_fuuid, 'rb') as fp:
            url_put_file = urljoin(url, f'/filehost/files/{fuuid}')
            # if file_size < 250 * 1024 * 1024:
            if file_size < 1:
                # One-shot upload
                async with session.put(url_put_file, data=fp, headers={'Content-Length': str(file_size)}) as r:
                    r.raise_for_status()
            else:
                # Parts upload
                updload_state = UploadState(fuuid, fp, file_size)
                await file_upload_parts(session, url_put_file, updload_state, batch_size=4096)
