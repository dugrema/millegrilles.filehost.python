import asyncio
import logging
import pathlib
import tempfile

from aiohttp import web
from io import BufferedReader
from typing import Optional, Union

from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie

CONST_BACKUP_ROTATION_INTERVAL = 3_600 * 24     # Once a day


class HostingBackupFileHandler:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

    async def run(self):
        await self.maintenance()

    async def maintenance(self):
        self.__logger.debug("Starting maintenance")
        await self.__backup_file_rotation_thread()
        self.__logger.debug("Stopping maintenance")

    async def __backup_file_rotation_thread(self):
        while self.__context.stopping is False:
            files_path = pathlib.Path(self.__context.configuration.dir_files)
            await self.__context.wait(CONST_BACKUP_ROTATION_INTERVAL)  # Every 24 hours

    async def put_backup_v2(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')

    async def get_backup_v2_domain_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')

    async def get_backup_v2_versions_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')

    async def get_backup_v2_archives_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')

    async def get_backup_v2(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')

    async def get_backup_v2_tar(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif 'fichiers' in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        raise NotImplementedError('todo')
