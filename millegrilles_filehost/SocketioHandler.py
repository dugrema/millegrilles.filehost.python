import aiohttp
import socketio
import logging

from typing import Optional
from socketio.exceptions import ConnectionRefusedError

from millegrilles_filehost.HostingFileHandler import HostingFileEventListener
from millegrilles_messages.messages import Constantes
from millegrilles_filehost.AuthenticationHandler import AuthenticationHandler
from millegrilles_filehost.Context import FileHostContext


class IdmgEventListener:

    def __init__(self, idmg: str):
        self.__idmg = idmg
        self.__sids = set()

    @property
    def idmg(self):
        return self.__idmg

    @property
    def sids(self):
        return self.__sids

    def add_sid(self, sid: str):
        self.__sids.add(sid)

    def remove_sid(self, sid: str):
        self.__sids.remove(sid)


class SioListeners(HostingFileEventListener):

    def __init__(self, sio: socketio.AsyncServer):
        super().__init__()
        self.__sio = sio

    async def on_event(self, idmg: str, name: str, value: dict):
        try:
            await self.__sio.emit(name, value, room='idmg/%s'%idmg)
        except KeyError:
            pass


class SocketioHandler:

    def __init__(self, context: FileHostContext, authentication_handler: AuthenticationHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__authentication_handler = authentication_handler
        self.__sio = socketio.AsyncServer(async_mode='aiohttp', cors_allowed_origins='*')
        self.__app: Optional[aiohttp.web_app.Application] = None
        self.__idmg_event_listener = SioListeners(self.__sio)

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

    async def __prepare_socketio_events(self):
        self.__sio.on('connect', handler=self.connect)
        self.__sio.on('disconnect', handler=self.disconnect)

    async def connect(self, sid: str, environ: dict, auth: Optional[dict] = None):
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
        # self.__sio_listeners.add_sid(idmg, sid)
        await self.__sio.enter_room(sid, room='idmg/%s'%idmg)

        return True

    async def disconnect(self, sid: str):
        self.__logger.debug("Disconnect sid %s" % sid)
        # self.__sio_listeners.remove_sid(sid)

