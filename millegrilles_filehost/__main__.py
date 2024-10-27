import asyncio
import logging

from millegrilles_filehost.Configuration import FileHostConfiguration
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.WebServer import WebServer


LOGGER = logging.getLogger(__name__)


async def main():
    config = FileHostConfiguration.load()
    context = FileHostContext(config)

    # Wire classes together, gets tasks to run
    tasks = wiring(context)

    # Run tasks. Any exception thrown by a task will stop the application cold.
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    if context.stopping is False:
        LOGGER.error("A task has stopped without the stop flag being set")
        # Stopping all threads
        context.stop()

    # Wait for all tasks to be complete. Ignore exceptions.
    results = await asyncio.gather(*pending, return_exceptions=True)
    for r in results:
        if isinstance(r, BaseException):
            LOGGER.warning("Exception on thread stop: %s" % r)


def wiring(context: FileHostContext) -> list[asyncio.Task]:
    # Create instances
    web_server = WebServer(context)

    # Wiring

    # Register listeners for the stop event
    context.register_stop_listener(web_server)

    # Create tasks
    threads = [
        asyncio.create_task(context.run()),
        asyncio.create_task(web_server.run())
    ]

    return threads


if __name__ == '__main__':
    asyncio.run(main())
