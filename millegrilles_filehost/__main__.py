import asyncio
import logging

from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.Configuration import FileHostConfiguration
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.HostingBackupFileHandler import HostingBackupFileHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler
from millegrilles_filehost.SocketioHandler import SocketioHandler
from millegrilles_filehost.WebRoutes import Handlers
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
    authentication_handler = AuthenticationHandler(context)
    hosting_file_handler = HostingFileHandler(context)
    backup_file_handler = HostingBackupFileHandler(context)
    socketio_handler = SocketioHandler(context, authentication_handler)

    handlers = Handlers(authentication_handler, hosting_file_handler, backup_file_handler, socketio_handler)
    web_server = WebServer(context, handlers)

    # Wiring
    socketio_handler.app = web_server.app
    sio_idmg_event_listener = socketio_handler.idmg_event_listener
    hosting_file_handler.add_event_listener(sio_idmg_event_listener)

    # Register listeners for the stop event
    # context.register_stop_listener(web_server)

    # Create tasks
    threads = [
        asyncio.create_task(context.run()),
        asyncio.create_task(web_server.run()),
        asyncio.create_task(authentication_handler.run()),
        asyncio.create_task(hosting_file_handler.run()),
        asyncio.create_task(backup_file_handler.run()),
        asyncio.create_task(socketio_handler.run()),
    ]

    return threads


if __name__ == '__main__':
    asyncio.run(main())
