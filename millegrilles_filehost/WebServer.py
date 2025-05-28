import asyncio
import logging

from aiohttp import web

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.WebRoutes import Handlers, WebRouteHandler

LOGGER = logging.getLogger(__name__)


async def error_middleware(app, handler):
    async def middleware_handler(request: web.Request):
        try:
            return await handler(request)
        except (ConnectionError, ConnectionResetError):
            LOGGER.info(f"Connection error/reset on request: {request.url}")
            return web.Response(status=100, text='Client disconnected - can retry')
        except Exception:
            LOGGER.exception('error_middleware Unexpected error')
            return web.Response(status=500, text='Internal Server Error')
    return middleware_handler


class WebServer:

    def __init__(self, context: FileHostContext, handlers: Handlers):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__handlers = handlers

        self.__web_app = web.Application(middlewares=[error_middleware])

        self.__web_sem = asyncio.BoundedSemaphore(3)

    @property
    def app(self):
        return self.__web_app

    async def stop(self):
        await self.__web_app.cleanup()

    async def run(self):
        self.__logger.debug("Web server starting")

        WebRouteHandler.prepare_routes(self.__context, self.__web_app, self.__handlers)
        runner = web.AppRunner(self.__web_app)
        await runner.setup()

        # Configure web port, SSL
        port = self.__context.configuration.web_port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__context.ssl_context)

        try:
            await site.start()
            self.__logger.info("Web server running, listening on port %s" % port)
            await self.__context.wait()  # Block while app is running
        finally:
            await self.stop()

        self.__logger.info("Web server stopping")
