import asyncio
import logging

from aiohttp import web

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler

LOGGER = logging.getLogger(__name__)


class Handlers:

    def __init__(self, authentication_handlers: AuthenticationHandler, hosting_file_handler: HostingFileHandler):
        self.__authentication_handlers = authentication_handlers
        self.__hosting_file_handler = hosting_file_handler
        self.semaphore_auth = asyncio.BoundedSemaphore(value=3)
        self.semaphore_web = asyncio.BoundedSemaphore(value=5)
        self.semaphore_file_put = asyncio.BoundedSemaphore(value=3)
        self.semaphore_file_get = asyncio.BoundedSemaphore(value=3)

    @property
    def authentication_handlers(self):
        return self.__authentication_handlers

    @property
    def hosting_file_handler(self):
        return self.__hosting_file_handler


class WebRouteHandler:

    def __init__(self, context: FileHostContext, handlers: Handlers):
        self.__context = context
        self.__handlers = handlers

    @staticmethod
    def prepare_routes(context: FileHostContext, web_app: web.Application, handlers: Handlers):
        handler = WebRouteHandler(context, handlers)
        web_app.add_routes([
            web.get('/filehost/status', handler.handle_get),

            web.post('/filehost/authenticate', handler.authenticate),
            web.get('/filehost/logout', handler.authenticate),

            # /files
            web.get('/filehost/files', handler.get_file_list),
            web.get('/filehost/files/{fuuid}', handler.get_file),
            web.put('/filehost/files/{fuuid}', handler.put_file),
            web.delete('/filehost/files/{fuuid}', handler.delete_file),

            # /backup
        ])

    async def handle_get(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_web:
            return web.json_response({"ok": True})

    async def authenticate(self, request: web.Request) -> web.Response :
        async with self.__handlers.semaphore_auth:
            return await self.__handlers.authentication_handlers.authenticate(request)

    async def logout(self, request: web.Request) -> web.Response :
        async with self.__handlers.semaphore_auth:
            return await self.__handlers.authentication_handlers.logout(request)

    async def get_file_list(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_web:
            return web.HTTPUnauthorized()

    async def get_file(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_file_get:
            return web.HTTPUnauthorized()

    async def put_file(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_file_put:
            return web.HTTPUnauthorized()

    async def delete_file(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_web:
            return web.HTTPUnauthorized()
