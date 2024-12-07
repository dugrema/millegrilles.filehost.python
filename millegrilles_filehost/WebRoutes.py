import asyncio
import logging

from aiohttp import web
from nacl.exceptions import CryptoError

from millegrilles_filehost import Constants
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.CookieUtilities import decrypt_cookie, Cookie, CookieExpired, verify_jwt
from millegrilles_filehost.HostingBackupFileHandler import HostingBackupFileHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler
from millegrilles_filehost.SocketioHandler import SocketioHandler

LOGGER = logging.getLogger(__name__)


class Handlers:

    def __init__(self, authentication_handlers: AuthenticationHandler,
                 hosting_file_handler: HostingFileHandler,
                 hosting_backup_file_handler: HostingBackupFileHandler,
                 socketio_handler: SocketioHandler):

        self.__authentication_handlers = authentication_handlers
        self.__hosting_file_handler = hosting_file_handler
        self.__hosting_backup_file_handler = hosting_backup_file_handler
        self.__socketio_handler = socketio_handler

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

    @property
    def hosting_backup_file_handler(self):
        return self.__hosting_backup_file_handler

    @property
    def socketio_hancler(self):
        return self.__socketio_handler


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
            web.post('/filehost/authenticate_jwt', handler.authenticate_jwt),
            web.get('/filehost/logout', handler.logout),
            web.get('/filehost/usage', handler.get_usage),

            # /files
            web.get('/filehost/files', handler.get_file_list),
            web.get('/filehost/files/{fuuid}', handler.get_file),
            web.put('/filehost/files/{fuuid}', handler.put_file),
            web.put('/filehost/files/{fuuid}/{position}', handler.put_file_part),
            web.post('/filehost/files/{fuuid}', handler.finish_file),
            web.delete('/filehost/files/{fuuid}', handler.delete_file),

            # /backup_v2
            # web.put('/filehost/backup_v2/{domain}/{file_type}/{filename}', handler.put_backup_v2),
            web.put('/filehost/backup_v2/{domain}/{file_type}/{version}/{filename}', handler.put_backup_v2),
            web.get('/filehost/backup_v2/domaines', handler.get_backup_v2_domain_list),
            web.get('/filehost/backup_v2/{domain}/archives', handler.get_backup_v2_versions_list),
            # web.get('/filehost/backup_v2/{domain}/final', handler.get_backup_v2_archives_list),
            web.get('/filehost/backup_v2/{domain}/archives/{version}', handler.get_backup_v2_archives_list),
            # web.get('/filehost/backup_v2/{domain}/final/{filename}', handler.get_backup_v2),
            web.get('/filehost/backup_v2/{domain}/archives/{version}/{filename}', handler.get_backup_v2),
            web.get('/filehost/backup_v2/tar/{domain}', handler.get_backup_v2_tar),
        ])

    async def handle_get(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_web:
            return web.json_response({"ok": True})

    async def authenticate(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_auth:
            return await self.__handlers.authentication_handlers.authenticate(request)

    async def authenticate_jwt(self, request: web.Request) -> web.Response:
        async with self.__handlers.semaphore_auth:
            return await self.__handlers.authentication_handlers.authenticate_jwt(request)

    async def logout(self, request: web.Request) -> web.Response :
        async with self.__handlers.semaphore_auth:
            return await self.__handlers.authentication_handlers.logout(request)

    async def get_file_list(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.file_list(request, cookie)

    async def get_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_get:
            return await self.__handlers.hosting_file_handler.get_file(request, cookie)

    async def put_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.put_file(request, cookie)

    async def delete_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.delete_file(request, cookie)

    async def get_usage(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_web:
            return await self.__handlers.hosting_file_handler.get_usage(request, cookie)

    async def put_file_part(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.put_file_part(request, cookie)

    async def finish_file(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_file_put:
            return await self.__handlers.hosting_file_handler.finish_file(request, cookie)

    async def extract_authentication(self, request: web.Request) -> Cookie:
        jwt = request.headers.get('X-Token-Jwt') or request.query.get('jwt')
        cookie = request.cookies.get(Constants.CONST_SESSION_COOKIE_NAME)

        if jwt is not None:
            verified_values = verify_jwt(self.__context.private_jwt_key.public_key(), jwt)
        elif cookie is not None:
            verified_values = decrypt_cookie(self.__context.secret_cookie_key, cookie)
        else:
            raise ValueError()

        return verified_values

    # Backup V2
    async def put_backup_v2(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.put_backup_v2(request, cookie)

    async def get_backup_v2_domain_list(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.get_backup_v2_domain_list(request, cookie)

    async def get_backup_v2_versions_list(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.get_backup_v2_versions_list(request, cookie)

    async def get_backup_v2_archives_list(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.get_backup_v2_archives_list(request, cookie)

    async def get_backup_v2(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.get_backup_v2(request, cookie)

    async def get_backup_v2_tar(self, request: web.Request) -> web.Response:
        try:
            cookie = await self.extract_authentication(request)
        except (ValueError, CookieExpired, CryptoError):
            return web.HTTPUnauthorized()
        async with self.__handlers.semaphore_backup:
            return await self.__handlers.hosting_backup_file_handler.get_backup_v2_tar(request, cookie)
