import asyncio

import aiohttp
import socketio
import logging
import pathlib
import json

from typing import Optional
from socketio.exceptions import ConnectionRefusedError

from millegrilles_filehost.HostingFileHandler import HostingFileEventListener, get_file_usage
from millegrilles_filehost.HostingFileTransfers import HostfileFileTransfers
from millegrilles_messages.messages import Constantes
from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.Context import FileHostContext, StopListener


class SioListeners(HostingFileEventListener):

    def __init__(self, sio: socketio.AsyncServer):
        super().__init__()
        self.__sio = sio

    async def on_event(self, idmg: str, name: str, value: dict):
        try:
            await self.__sio.emit(name, value, room='idmg/%s'%idmg)
        except KeyError:
            pass


class SocketioHandler(StopListener):

    def __init__(self, context: FileHostContext, authentication_handler: AuthenticationHandler, hosting_filetransfers: HostfileFileTransfers):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__authentication_handler = authentication_handler
        self.__hosting_filetransfers = hosting_filetransfers
        self.__sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
        self.__app: Optional[aiohttp.web_app.Application] = None
        self.__idmg_event_listener = SioListeners(self.__sio)
        self.__sids_idmg: dict[str, str] = dict()  # {sid: idmg}

    @property
    def app(self):
        return self.__app

    @app.setter
    def app(self, value):
        self.__app = value
        socketio_path = f'/filehost/socket.io'
        self.__sio.attach(self.__app, socketio_path=socketio_path)

    @property
    def idmg_event_listener(self):
        return self.__idmg_event_listener

    async def run(self):
        await self.__prepare_socketio_events()
        while self.__context.stopping is False:
            await self.__context.wait()

    async def stop(self):
        await self.__sio.shutdown()

    async def __prepare_socketio_events(self):
        self.__sio.on('connect', handler=self.on_connect)
        self.__sio.on('disconnect', handler=self.on_disconnect)

        self.__sio.on('usage', handler=self.on_usage)
        self.__sio.on('transfer_put', handler=self.__on_add_file_transfer)
        self.__sio.on('transfer_get', handler=self.__on_add_file_transfer)

    async def on_connect(self, sid: str, environ: dict, auth: Optional[dict] = None):
        if auth is None:
            self.__logger.debug("Missing authentication message - REFUSED")
            raise ConnectionRefusedError('authentication failed')

        try:
            enveloppe = await self.__authentication_handler.verify_auth_message(auth)
            idmg = enveloppe.idmg

            roles = enveloppe.get_roles
            exchanges = enveloppe.get_exchanges
            if 'filecontroler' in roles and Constantes.SECURITE_PUBLIC in exchanges:
                pass  # Ok, filecontroler
            else:
                self.__logger.debug("Message valid, certificate credentials wrong - REFUSED")
                raise ConnectionRefusedError('authentication failed')
        except ConnectionRefusedError as e:
            raise e
        except:
            self.__logger.exception("Error on authentication")
            raise ConnectionRefusedError('authentication failed')

        self.__logger.debug("Connected sid %s" % sid)
        await self.__sio.enter_room(sid, room='idmg/%s'%idmg)

        # Keep link between sid and idmg for commands
        self.__sids_idmg[sid] = idmg

        return True

    async def on_disconnect(self, sid: str):
        self.__logger.debug("Disconnect sid %s" % sid)

        # Remove sid from dict
        try:
            del self.__sids_idmg[sid]
        except KeyError:
            pass

    async def on_usage(self, sid: str):
        try:
            idmg = self.__sids_idmg[sid]
        except KeyError:
            return {'ok': False}  # Access denied

        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return {'ok': False}  # Access denied

        try:
            usage = await get_file_usage(path_idmg, self.__context.semaphore_usage_update)
        except FileNotFoundError:
            return {'ok': False, 'err': 'No information'}
        else:
            return {'ok': True, 'usage': usage}

    async def __on_add_file_transfer(self, sid: str, command: dict):
        try:
            enveloppe = await self.__authentication_handler.verify_auth_message(command)
            idmg = enveloppe.idmg

            # Ensure that IDMG exists
            path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
            if path_idmg.exists() is False:
                return {'ok': False, 'err': 'Access denied', 'code': 403}  # Access denied

            action = command['routage']['action']

            content = json.loads(command['contenu'])

            if action == 'putFile':
                # Ensure that fuuid exists locally
                fuuid = content['fuuid']
                path_fuuid = pathlib.Path(path_idmg, 'buckets', fuuid[-2:], fuuid)
                if path_fuuid.exists() is False:
                    return {'ok': False, 'err': 'File not found', 'code': 404}  # Access denied

            transfer_command = {'command': command, 'idmg': idmg, 'enveloppe': enveloppe, 'action': action, 'content': content}

            await self.__hosting_filetransfers.add_transfer(transfer_command)
            return {'ok': True}
        except asyncio.QueueFull:
            return {'ok': False, 'err': 'Queue full, try again later'}
        except Exception as e:
            self.__logger.exception("Unhandled exception")
            return {'ok': False, 'err': str(e)}
