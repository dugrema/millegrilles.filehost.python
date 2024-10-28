import asyncio
import logging
import pathlib
import tempfile

from io import BufferedReader
from typing import Optional

from millegrilles_filehost.Context import FileHostContext


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

    async def put_backup_v2_fichier(self, fichier_temp: tempfile.TemporaryFile,
                                    domaine: str, nom_fichier: str, type_fichier: str, version: Optional[str] = None):
        """
        :param fichier_temp:
        :param domaine:
        :param nom_fichier:
        :param type_fichier: F pour final, C pour concatene, I pour incremental
        :param version: Si absent, doit etre un fichier final. Pour C et I doit etre present.
        :return:
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_fichier_stream(self, domaine: str, nom_fichier: str, version: Optional[str] = None) -> BufferedReader:
        """
        :param domaine:
        :param nom_fichier: Fichier .mgbak
        :param version: Si absent, retourne un fichier final. Sinon utilise la version d'archive specifiee.
        :return:
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_versions(self, domaine: str) -> dict:
        """
        :param domaine:
        :return: Liste de version disponibles pour ce domaine en format JSON. Identifie la versions courante.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_list(self, domaine, version: Optional[str] = None) -> list[str]:
        """
        Retourne une liste de fichiers du domaine specifie. Seul le nom des fichiers et fourni (pas de repertoire).
        :param domaine:
        :param version: Si aucune version n'est fournie, retourne la liste des fichiers finaux du domaine.
        :return: Liste texte utf-8 avec un fichier par ligne.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_headers(self, domaine, version: Optional[str] = None):
        """
        Retourne les headers presents dans le backup sous forme de 1 header JSON par ligne.
        :param domaine:
        :param version: Si absent, utilise les fichiers finaux. Sinon charge toutes les archives de la version specifiee.
        :return: Liste de headers json, 1 par ligne.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_domaines(self, domaines: Optional[list[str]] = None, courant=True, stats=False, cles=False):
        """
        Retourne une liste des domaines avec information
        :param domaines: Liste optionnelle de domaines a charger. Si None, retourne tous les domaines.
        :param courant: Si True, retourne uniquement les backup courants.
        :param stats:
        :param cles: Si True, inclue un dict de cles sous format: {cle_id: DomaineSignature}
        :return: Liste de domaines format: {domaine, date, version, nombre_transactions}
        """
        raise NotImplementedError('must override')

    async def rotation_backups_v2(self, nombre_archives=3):
        raise NotImplementedError('must override')

    def get_path_backup_v2(self) -> pathlib.Path:
        raise NotImplementedError('must override')
