import asyncio
import logging
import ssl
import signal

from ssl import SSLContext, VerifyMode

from typing import Optional

from millegrilles_filehost.Configuration import FileHostConfiguration

LOGGER = logging.getLogger(__name__)


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

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.debug("Signal received: %d, closing" % signum)
        self.stop()

    def stop(self):
        loop = asyncio.get_event_loop()
        if loop is not None:
            loop.call_soon_threadsafe(self.__stop_event.set)
        else:
            self.__logger.warning("Stopping without asyncio loop, may take time")
            self.__stop_event.set()

    async def run(self):
        await self.__stop_thread()

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
