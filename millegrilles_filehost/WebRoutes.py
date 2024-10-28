import asyncio
import logging

from aiohttp import web

from millegrilles_filehost import Constants
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.CookieUtilities import decrypt_cookie, Cookie, CookieExpired
from millegrilles_filehost.HostingBackupFileHandler import HostingBackupFileHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler

LOGGER = logging.getLogger(__name__)


class Handlers:

    def __init__(self, authentication_handlers: AuthenticationHandler,
                 hosting_file_handler: HostingFileHandler,
                 hosting_backup_file_handler: HostingBackupFileHandler):
        self.__authentication_handlers = authentication_handlers
        self.__hosting_file_handler = hosting_file_handler
        self.__hosting_backup_file_handler = hosting_backup_file_handler
        self.semaphore_auth = asyncio.BoundedSemaphore(value=3)
        self.semaphore_web = asyncio.BoundedSemaphore(value=5)
        self.semaphore_file_put = asyncio.BoundedSemaphore(value=5)
        self.semaphore_file_get = asyncio.BoundedSemaphore(value=20)
        self.semaphore_backup = asyncio.BoundedSemaphore(value=5)

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
            web.get('/filehost/logout', handler.logout),

            # /files
            web.get('/filehost/files', handler.get_file_list),
            web.get('/filehost/files/{fuuid}', handler.get_file),
            web.get('/filehost/usage', handler.get_usage),
            web.put('/filehost/files/{fuuid}', handler.put_file),
            # File parts
            web.put('/filehost/files/{fuuid}/{position}', handler.put_file_part),
            web.post('/filehost/files/{fuuid}', handler.finish_file),
            # Cleanup
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
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.file_list(request, cookie)

    async def get_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_get:
            return await self.__handlers.hosting_file_handler.get_file(request, cookie)

    async def put_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.put_file(request, cookie)

    async def delete_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.delete_file(request, cookie)

    async def get_usage(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.get_usage(request, cookie)

    async def put_file_part(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.put_file_part(request, cookie)

    async def finish_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.decrypt_cookie(request)
        except (ValueError, CookieExpired):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.finish_file(request, cookie)

    async def decrypt_cookie(self, request: web.Request) -> Cookie:
        cookie = request.cookies.get(Constants.CONST_SESSION_COOKIE_NAME)
        if cookie is None:
            raise ValueError()
        decrypted_cookie = decrypt_cookie(self.__context.secret_cookie_key, cookie)
        return decrypted_cookie
