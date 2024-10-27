import asyncio
import logging

from aiohttp import web

from millegrilles_filehost.Context import FileHostContext, StopListener


class WebServer(StopListener):

    def __init__(self, context: FileHostContext):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

        self.__web_app = web.Application()

        self.__web_sem = asyncio.BoundedSemaphore(3)

    async def stop(self):
        await self.__web_app.cleanup()

    async def run(self):
        self.__logger.debug("Web server starting")

        self._prepare_routes()

        runner = web.AppRunner(self.__web_app)
        await runner.setup()

        # Configure web port, SSL
        port = self.__context.configuration.web_port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__context.ssl_context)

        await site.start()
        self.__logger.info("Web server running, listening on port %s" % port)
        await self.__context.wait()

        self.__logger.info("Web server stopping")

    def _prepare_routes(self):
        self.__web_app.add_routes([
            # /fichiers_transfert
            web.get('/filehost', self.handle_get),
        ])

    async def handle_get(self, request: web.Request) -> web.Response:
        self.__logger.debug("GET")
        async with self.__web_sem:
            return web.json_response({"ok": True})
