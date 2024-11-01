import asyncio
import logging
import ssl
import signal
import secrets
import threading

from ssl import SSLContext, VerifyMode

from typing import Optional

from millegrilles_filehost.Configuration import FileHostConfiguration

LOGGER = logging.getLogger(__name__)

# Use random value for cookie key - will be overridden if configured to use external source
SECRET_COOKIE_KEY = secrets.token_bytes(32)


class StopListener:
    """
    Extend this class to receive a stop callback when the application is stopping
    """

    def __init__(self):
        pass

    async def stop(self):
        # Hook
        pass


class FileHostContext:

    def __init__(self, configuration: FileHostConfiguration):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration
        self.__stop_event = asyncio.Event()
        self.__stop_listeners: list[StopListener] = list()
        self.__ssl_context = _load_ssl_context(configuration)
        self.__secret_cookie_key = SECRET_COOKIE_KEY
        self.__sync_event = threading.Event()

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.debug("Signal received: %d, closing" % signum)
        self.stop()

    def stop(self):
        self.__sync_event.set()

    async def run(self):
        await asyncio.gather(self.__stop_thread(), self.__sync_stop_thread())

    async def __sync_stop_thread(self):
        """
        Thread that listens to a non async process/callback toggling the sync_event flag.
        :return:
        """
        await asyncio.to_thread(self.__sync_event.wait)
        self.__stop_event.set()  # Toggle async stop thread

    async def __stop_thread(self):
        await self.__stop_event.wait()
        for listener in self.__stop_listeners:
            try:
                await listener.stop()
            except Exception:
                self.__logger.exception("Error stopping listener %s" % listener)

    async def wait(self, duration: Optional[int] = None):
        """
        Utility for waiting on the stop event.
        :param duration:
        :return:
        """
        if duration:
            try:
                await asyncio.wait_for(self.__stop_event.wait(), duration)
            except asyncio.TimeoutError:
                pass
        else:
            await self.__stop_event.wait()

    @property
    def stopping(self):
        return self.__stop_event.is_set()

    def register_stop_listener(self, listener: StopListener):
        """
        Register to get a notification when stopping the application.
        :param listener:
        :return:
        """
        self.__stop_listeners.append(listener)

    @property
    def configuration(self):
        return self.__configuration

    @property
    def ssl_context(self):
        return self.__ssl_context

    @property
    def secret_cookie_key(self):
        return self.__secret_cookie_key


def _load_ssl_context(configuration: FileHostConfiguration) -> ssl.SSLContext:
    ssl_context = SSLContext()

    LOGGER.debug("Load web certificate %s" % configuration.web_cert_path)
    ssl_context.load_cert_chain(configuration.web_cert_path,
                                configuration.web_key_path)

    if configuration.web_ca_path:
        ssl_context.load_verify_locations(cafile=configuration.web_ca_path)
        ssl_context.verify_mode = VerifyMode.CERT_OPTIONAL
    else:
        ssl_context.verify_mode = VerifyMode.CERT_NONE

    return ssl_context
