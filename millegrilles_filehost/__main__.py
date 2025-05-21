import asyncio
import logging
import os
import pathlib
# import multiprocessing as mp

from asyncio import TaskGroup

from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.Configuration import FileHostConfiguration
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.HostingBackupFileHandler import HostingBackupFileHandler
from millegrilles_filehost.HostingFileHandler import HostingFileHandler
from millegrilles_filehost.HostingFileTransfers import HostfileFileTransfersFuuids, HostfileFileTransfersBackup
from millegrilles_filehost.SocketioHandler import SocketioHandler
from millegrilles_filehost.WebRoutes import Handlers
from millegrilles_filehost.WebServer import WebServer
from millegrilles_messages.bus.BusContext import ForceTerminateExecution

LOGGER = logging.getLogger(__name__)

# mp.set_start_method('spawn')

async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = FileHostConfiguration.load()
    context = FileHostContext(config)

    create_folders(context)

    # Wire classes together, gets awaitables to run
    coros = wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


def wiring(context: FileHostContext):
    # Create instances
    authentication_handler = AuthenticationHandler(context)
    hosting_file_handler = HostingFileHandler(context)
    backup_file_handler = HostingBackupFileHandler(context)
    hosting_file_transfers = HostfileFileTransfersFuuids(context, hosting_file_handler)
    hosting_backup_file_transfers = HostfileFileTransfersBackup(context, backup_file_handler)
    socketio_handler = SocketioHandler(context, authentication_handler, hosting_file_transfers, hosting_backup_file_transfers, hosting_file_handler)

    handlers = Handlers(authentication_handler, hosting_file_handler, backup_file_handler, socketio_handler)
    web_server = WebServer(context, handlers)

    # Wiring
    socketio_handler.app = web_server.app
    sio_idmg_event_listener = socketio_handler.idmg_event_listener
    hosting_file_handler.add_event_listener(sio_idmg_event_listener)
    hosting_file_transfers.set_event_callback(sio_idmg_event_listener.on_event)
    hosting_backup_file_transfers.set_event_callback(sio_idmg_event_listener.on_event)

    # Register listeners for the stop event
    # context.register_stop_listener(web_server)
    context.register_stop_listener(socketio_handler)

    # Create tasks
    threads = [
        context.run(),
        web_server.run(),
        authentication_handler.run(),
        hosting_file_handler.run(),
        backup_file_handler.run(),
        socketio_handler.run(),
        hosting_file_transfers.run(),
        hosting_backup_file_transfers.run(),
    ]

    return threads


def create_folders(context: FileHostContext):
    dir_files = pathlib.Path(context.configuration.dir_files)
    dir_files.mkdir(parents=True, exist_ok=True)

    try:
        idmg_default = os.environ['IDMG']
        path_idmg = dir_files.joinpath(idmg_default)
        path_idmg.mkdir(parents=True, exist_ok=True)
    except KeyError:
        pass


if __name__ == '__main__':
    asyncio.run(main())
