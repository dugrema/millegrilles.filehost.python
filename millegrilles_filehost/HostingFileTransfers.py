import asyncio
import logging
from asyncio import TaskGroup

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

            self.__logger.debug("Transferring: %s" % transfer)
