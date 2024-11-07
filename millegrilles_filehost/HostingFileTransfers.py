import asyncio
import logging
import json

from asyncio import TaskGroup
from urllib.parse import urljoin

import aiohttp

from millegrilles_filehost.Context import FileHostContext


class HostfileFileTransfers:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__transfer_queue = asyncio.Queue(maxsize=1)

    async def __stop_thread(self):
        await self.__context.wait()
        self.__transfer_queue.clear()
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

            action = transfer['routage']['action']
            if action == 'putFile':
                await self.__put_file(transfer)
            else:
                raise Exception('Unknown transfer action')

            self.__logger.debug("Transferring: %s" % transfer)


    async def __put_file(self, transfer: dict):
        content = json.loads(transfer['contenu'])
        fuuid = content['fuuid']
        url = content['url']
        tls_mode = content.get('tls') or 'external'

        self.__logger.debug("PUT file %s to %s (TLS: %s)" % (fuuid, url, tls_mode))

        verify = tls_mode != 'nocheck'
        connector = aiohttp.TCPConnector(verify_ssl=verify)
        async with aiohttp.ClientSession(connector=connector) as session:
            url_authentication = urljoin(url, '/filehost/authenticate')
            async with session.post(url_authentication, json=transfer) as r:
                r.raise_for_status()
            pass
        pass
