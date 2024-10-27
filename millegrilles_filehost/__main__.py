import asyncio
import logging

from millegrilles_filehost.Configuration import FileHostConfiguration
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.WebServer import WebServer


LOGGER = logging.getLogger(__name__)


async def main():
    config = FileHostConfiguration.load()
    context = FileHostContext(config)
    web_server = WebServer(context)

    # Register listeners for the stop event
    context.register_stop_listener(web_server)

    threads = [
        asyncio.create_task(context.run()),
        asyncio.create_task(web_server.run())
    ]

    done, pending = await asyncio.wait(threads, return_when=asyncio.FIRST_COMPLETED)

    if context.stopping is False:
        LOGGER.error("Exception - application thread has stopped without setting the stop flag")
        context.stop()

    # Wait for all tasks to be complete. Do not crash on exceptions.
    await asyncio.gather(*pending, return_exceptions=True)

    await asyncio.sleep(1)


if __name__ == '__main__':
    asyncio.run(main())
