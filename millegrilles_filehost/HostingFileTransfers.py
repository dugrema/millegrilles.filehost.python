import asyncio
import logging
import pathlib

from asyncio import TaskGroup
from urllib.parse import urljoin

import aiohttp

from millegrilles_filehost.Context import FileHostContext
from millegrilles_messages.utils.FilePartUploader import UploadState, file_upload_parts


class HostfileFileTransfers:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__transfer_queue = asyncio.Queue(maxsize=1)

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

            action = transfer['action']
            if action == 'putFile':
                try:
                    await self.__put_file(transfer)
                except:
                    self.__logger.exception("Unhandled transfer exception")
            else:
                self.__logger.error("Unknown transfer action: %s" % action)

            self.__logger.debug("Transferring: %s" % transfer)


    async def __put_file(self, transfer: dict):
        idmg = transfer['idmg']
        content = transfer['content']
        fuuid = content['fuuid']
        url = content['url']
        tls_mode = content.get('tls') or 'external'

        self.__logger.debug("PUT file %s to %s (TLS: %s)" % (fuuid, url, tls_mode))
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)

        stat_fuuid = path_fuuid.stat()
        file_size = stat_fuuid.st_size

        with open(path_fuuid, 'rb') as fp:
            verify = tls_mode != 'nocheck'
            connector = aiohttp.TCPConnector(verify_ssl=verify)
            async with aiohttp.ClientSession(connector=connector) as session:
                url_authentication = urljoin(url, '/filehost/authenticate')
                async with session.post(url_authentication, json=transfer['command']) as r:
                    r.raise_for_status()
                pass

                url_put_file = urljoin(url, f'/filehost/files/{fuuid}')
                # if file_size < 100 * 1024 * 1024:
                if file_size < 1:
                    # One-shot upload
                    async with session.put(url_put_file, data=fp, headers={'Content-Length': str(file_size)}) as r:
                        r.raise_for_status()
                else:
                    # Parts upload
                    updload_state = UploadState(fuuid, fp, file_size)
                    await file_upload_parts(session, url_put_file, updload_state, batch_size=4096)

                self.__logger.debug("PUT complete for file %s to %s" % (fuuid, url))

        pass
