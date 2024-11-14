import asyncio
import logging
import pathlib
import datetime
import json
import math

from aiohttp import web
from typing import Union

from millegrilles_messages.messages import Constantes
from millegrilles_filehost.BackupV2 import lire_header_archive_backup, get_backup_v2_domaines, extraire_headers
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import Cookie
from millegrilles_messages.messages.Hachage import Hacheur
from millegrilles_messages.utils.TarStream import stream_path_to_tar_async

CONST_BACKUP_ROTATION_INTERVAL = 3_600 * 24     # Once a day
CONST_ROLE_FILECONTROLER = 'filecontroler'

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

    def get_backup_file(self, idmg: str, domain: str, version: str, file: str):
        return pathlib.Path(self.__context.configuration.dir_files, idmg, 'backup_v2', domain, version, file)

    async def put_backup_v2(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        domains = cookie.get('domaines')
        if '4.secure' in cookie.get('exchanges') and domains is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        domain = request.match_info['domain']
        file_type = request.match_info['file_type']
        version = request.match_info['version']
        filename: str = request.match_info['filename']

        if domains and domain not in domains:
            # This is not a file manager and it is trying to put a new file for a domain it does not control - REJECT
            return web.HTTPForbidden()

        if file_type not in ['final', 'concatene', 'incremental']:
            self.__logger.info("put_backup_v2 Unknown file type : %s" % file_type)
            return web.HTTPBadRequest()

        # Check if the file already exists
        path_domain = pathlib.Path(path_idmg, 'backup_v2', domain)
        path_version = pathlib.Path(path_domain, version)
        path_file = pathlib.Path(path_version, filename)
        path_file_work = pathlib.Path(path_version, filename + '.work')
        if path_file.exists():
            return web.HTTPConflict()  # File already received

        # Ensure the directory exists or can be created
        await asyncio.to_thread(path_version.mkdir, parents=True, exist_ok=True)

        self.__logger.debug("handle_put_backup_v2 %s/%s/%s/%s" % (domain, file_type, version, filename))
        suffix_digest_file = filename.split('.')[0].split('_').pop()

        # Receive into tempfile
        digester = Hacheur('blake2b-512', 'base58btc')

        try:
            with open(path_file_work, 'wb') as output:
                async for chunk in request.content.iter_chunked(64*1024):
                    output.write(chunk)
                    digester.update(chunk)

                digest_result = digester.finalize()
                self.__logger.debug("handle_put_backup_v2 Digest result for file %s = %s" % (filename, digest_result))
                if digest_result.endswith(suffix_digest_file) is False:
                    self.__logger.error("handle_put_backup_v2 Digest du fichier mismatch : %s" % filename)
                    return web.HTTPBadRequest()

            with open(path_file_work, 'rb') as output:
                # Lire le header du fichier de backup
                header_archive = await asyncio.to_thread(lire_header_archive_backup, output)

            if domain != header_archive['domaine']:
                self.__logger.error("handle_put_backup_v2 Mismatch de domaine fourni %s et celui du header : %s" % (domain, filename))
                return web.HTTPBadRequest()

            # Confirmer le type et domaine
            char_type_archive = header_archive['type_archive']
            if file_type == 'final' and char_type_archive != 'F':
                self.__logger.error("handle_put_backup_v2 Type de fichier doit etre F pour final : %s" % filename)
                return web.HTTPBadRequest()
            elif file_type == 'concatene' and char_type_archive != 'C':
                self.__logger.error("handle_put_backup_v2 Type de fichier doit etre C pour final : %s" % filename)
                return web.HTTPBadRequest()
            elif file_type == 'incremental' and char_type_archive != 'I':
                self.__logger.error("handle_put_backup_v2 Type de fichier doit etre I pour final : %s" % filename)
                return web.HTTPBadRequest()

            # S'assurer que le fichier de backup est pour le system courant (idmg)
            if header_archive['idmg'] != idmg:
                self.__logger.error("put_backup_v2_fichier Fichier upload pour le mauvais IDMG")
                return web.HTTPExpectationFailed()

            if header_archive['type_archive'] in ['C', 'F']:
                # S'assurer que le repertoire existe
                path_version.mkdir(parents=True, exist_ok=True)

            # All good, move backup file
            path_file_work.rename(path_file)

            if header_archive['type_archive'] == 'C':
                await self.create_info_files(path_domain, header_archive, version)
                # # Nouveau fichier concatene, on met a jour la version courante
                # # info_version = {'version': version, 'date': int(datetime.datetime.now(datetime.UTC).timestamp())}
                # fin_backup_secs = math.floor(header_archive['fin_backup'] / 1000)
                # info_version = {'version': version, 'date': fin_backup_secs}
                # path_fichier_info = pathlib.Path(path_version, 'info.json')
                # with open(path_fichier_info, 'wt') as fichier:
                #     await asyncio.to_thread(json.dump, info_version, fichier)
                #
                # # Remplacer le fichier courant.json
                # path_fichier_courant = pathlib.Path(path_domain, 'courant.json')
                # path_fichier_courant.unlink(missing_ok=True)
                # with open(path_fichier_info, 'rb') as src:
                #     content = await asyncio.to_thread(src.read)
                # with open(path_fichier_courant, 'wb') as output:
                #     await asyncio.to_thread(output.write, content)
        finally:
            # Cleanup
            await asyncio.to_thread(path_file_work.unlink, missing_ok=True)

        return web.HTTPOk()

    async def create_info_files(self, path_domain: pathlib.Path, header_archive: dict, version: str):
        # Nouveau fichier concatene, on met a jour la version courante
        path_version = pathlib.Path(path_domain, version)
        fin_backup_secs = math.floor(header_archive['fin_backup'] / 1000)
        info_version = {'version': version, 'date': fin_backup_secs}
        path_fichier_info = pathlib.Path(path_version, 'info.json')
        with open(path_fichier_info, 'wt') as fichier_info:
            await asyncio.to_thread(json.dump, info_version, fichier_info)

        # Remplacer le fichier courant.json
        path_fichier_courant = pathlib.Path(path_domain, 'courant.json')
        path_fichier_courant.unlink(missing_ok=True)
        with open(path_fichier_info, 'rb') as src:
            content = await asyncio.to_thread(src.read)
        with open(path_fichier_courant, 'wb') as output:
            await asyncio.to_thread(output.write, content)

    async def get_backup_v2_domain_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        path_backup = pathlib.Path(path_idmg, 'backup_v2')
        stats = request.query.get('stats') == 'true'
        cles = request.query.get('cles') == 'true'
        version = request.query.get('version')
        domains = await get_backup_v2_domaines(path_backup, stats=stats, cles=cles, version=version)
        return web.json_response({'domaines': domains})

    async def get_backup_v2_versions_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        domain = request.match_info['domain']

        path_backup = pathlib.Path(path_idmg, 'backup_v2')
        path_domaine = pathlib.Path(path_backup, domain)
        path_info_courante = pathlib.Path(path_domaine, 'courant.json')
        versions = list()

        try:
            with open(path_info_courante, 'rt') as fichier:
                current_version = json.load(fichier)
            for path_uuid_backup in path_domaine.iterdir():
                if path_uuid_backup.is_dir():
                    path_info = pathlib.Path(path_domaine, path_uuid_backup, 'info.json')
                    try:
                        with open(path_info, 'rt') as fichier:
                            info_version = json.load(fichier)

                        headers = extraire_headers(path_uuid_backup)
                        transactions_total = 0
                        transactions_concatene = 0
                        concatene_start_date = 0
                        concatene_end_date = 0
                        end_date = 0
                        for h in headers:
                            transactions_total += h.get('nombre_transactions')
                            if h.get('type_archive') == 'C':
                                transactions_concatene += h.get('nombre_transactions')
                                concatene_start_date = int(h.get('debut_backup') / 1000)
                                concatene_end_date = int(h.get('fin_backup') / 1000)
                            if end_date < h.get('fin_backup'):
                                end_date = h['fin_backup']

                        info_version['start_date'] = concatene_start_date
                        info_version['end_date'] = int(end_date / 1000)
                        info_version['end_date_concatene'] = concatene_end_date
                        info_version['transactions'] = transactions_total
                        info_version['transactions_concatene'] = transactions_concatene

                        versions.append(info_version)

                    except FileNotFoundError:
                        self.__logger.info("Version backup %s : aucun fichier info.json" % path_info)
                    except json.JSONDecodeError:
                        self.__logger.warning("Version backup %s : info.json corrompu" % path_info)
        except FileNotFoundError:
            # New domain without a folder or no courant.json file
            current_version = None

        return web.json_response({"versions": versions, "version_courante": current_version})

    async def get_backup_v2_archives_list(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        domain: str = request.match_info['domain']
        version: str = request.match_info['version']

        path_backup_version = pathlib.Path(path_idmg, 'backup_v2', domain, version)
        if path_backup_version.exists() is False:
            return web.HTTPNotFound()

        self.__logger.debug("handle_get_backup_v2_liste_archives domain %s, version %s" % (domain, version))
        headers_response = {
            # 'Cache-Control': 'public, max-age=604800, immutable',
        }
        response = web.StreamResponse(status=200, headers=headers_response)
        await response.prepare(request)

        for filename in backup_file_iter(path_backup_version):
            await response.write(filename.encode('utf-8') + b'\n')

        await response.write_eof()
        return response

    async def get_backup_v2(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        if '4.secure' in cookie.get('exchanges') and cookie.get('domaines') is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in cookie.get('roles'):
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        domain: str = request.match_info['domain']
        version: str = request.match_info['version']
        filename: str = request.match_info['filename']
        file_path = pathlib.Path(path_idmg, 'backup_v2', domain, version, filename)

        try:
            stat = file_path.stat()
        except FileNotFoundError:
            return web.HTTPNotFound()
        size = stat.st_size

        with open(file_path, 'rb') as in_file:
            response = web.StreamResponse(status=200)
            response.content_length = size
            response.content_type = 'application/octet-stream'
            await response.prepare(request)
            while True:
                chunk = await asyncio.to_thread(in_file.read, 64*1024)
                if len(chunk) == 0:
                    break
                await response.write(chunk)
            await response.write_eof()

        return response

    async def get_backup_v2_tar(self, request: web.Request, cookie: Cookie) -> Union[web.Response, web.StreamResponse]:
        # This is a read-write/admin level function. Ensure proper roles/security level
        exchanges = cookie.get('exchanges') or list()
        domaines = cookie.get('domaines') or list()
        roles = cookie.get('roles') or list()
        delegation_globale = cookie.get('delegation_globale')
        if '4.secure' in exchanges and domaines is not None:
            pass
        elif CONST_ROLE_FILECONTROLER in roles:
            pass
        elif delegation_globale == Constantes.DELEGATION_GLOBALE_PROPRIETAIRE:
            pass
        else:
            return web.HTTPForbidden()

        idmg = cookie.idmg
        path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
        if path_idmg.exists() is False:
            return web.HTTPForbidden()  # IDMG is not hosted

        domain: str = request.match_info['domain']
        path_backup_v2 = pathlib.Path(path_idmg, 'backup_v2')

        # Trouver path du backup courant
        fichier_courant_path = pathlib.Path(path_backup_v2, domain, 'courant.json')
        try:
            with open(fichier_courant_path) as fichier:
                info_courant = json.load(fichier)
            version_courante = info_courant['version']
            version_courante_path = pathlib.Path(path_backup_v2, domain, version_courante)
        except FileNotFoundError:
            return web.HTTPNotFound()

        if version_courante_path.exists() is False:
            return web.HTTPNotFound()

        date_transactions_epochms = 0
        headers = extraire_headers(version_courante_path)
        for h in headers:
            if date_transactions_epochms < h['fin_backup']:
                date_transactions_epochms = h['fin_backup']
        date_transactions = datetime.datetime.fromtimestamp(date_transactions_epochms / 1000)
        date_transactions_formattee = date_transactions.strftime("%Y%m%d%H%M%S")

        nom_fichier = f"backup_{idmg}_{domain}_{date_transactions_formattee}_{version_courante}.tar"
        headers_response = {'Content-Disposition': f'attachment; filename="{nom_fichier}"'}
        response = web.StreamResponse(status=200, headers=headers_response)
        response.content_type = 'application/x-tar'
        await response.prepare(request)

        await stream_path_to_tar_async(version_courante_path, response)

        await response.write_eof()
        return response


def backup_file_iter(path_archives: pathlib.Path):
    for file in path_archives.iterdir():
        if file.is_file() and file.suffix == ".mgbak":
            yield file.name
