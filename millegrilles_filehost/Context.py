import asyncio
import logging

from typing import Optional

from millegrilles_filehost.Configuration import FileHostConfiguration


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

    def stop(self):
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
