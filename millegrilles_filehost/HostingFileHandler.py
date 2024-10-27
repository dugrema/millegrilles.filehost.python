import logging

from aiohttp import web
from typing import Union

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie


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
        idmg = cookie.idmg

        response = web.StreamResponse(status=200)
        await response.prepare(request)

        await response.write(b"pas de fichiers!\n")
        await response.write(b"deuxieme pas de fichiers!\n")
        await response.write(b"troisieme pas de fichiers!\n")

        await response.write_eof()
        return response
