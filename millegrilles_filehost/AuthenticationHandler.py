import logging

from aiohttp import web

from millegrilles_filehost.Context import FileHostContext


class AuthenticationHandler:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

    async def run(self):
        while self.__context.stopping is False:
            await self.maintenance()
            await self.__context.wait(30)

    async def maintenance(self):
        self.__logger.debug("Maintenance cycle")

    async def authenticate(self, request: web.Request) -> web.Response :
        return web.HTTPForbidden()

    async def logout(self, request: web.Request) -> web.Response :
        return web.HTTPForbidden()
