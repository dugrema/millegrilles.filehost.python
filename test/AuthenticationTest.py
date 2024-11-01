import asyncio
import aiohttp
import pathlib
import logging

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.utils.FilePartUploader import file_upload_parts, UploadState

PATH_FICHIERS_CERT = '/var/opt/millegrilles/secrets/pki.fichiers.cert'
PATH_FICHIERS_CLE = '/var/opt/millegrilles/secrets/pki.fichiers.cle'
PATH_CORE_CERT = '/var/opt/millegrilles/secrets/pki.core.cert'
PATH_CORE_CLE = '/var/opt/millegrilles/secrets/pki.core.cle'
PATH_CA_CERT = '/var/opt/millegrilles/configuration/pki.millegrille.cert'

logger = logging.getLogger(__name__)


def load_formatter_fichiers() -> (SignateurTransactionSimple, FormatteurMessageMilleGrilles):
    clecert = CleCertificat.from_files(PATH_FICHIERS_CLE, PATH_FICHIERS_CERT)
    enveloppe = clecert.enveloppe
    idmg = enveloppe.idmg

    ca = EnveloppeCertificat.from_file(PATH_CA_CERT)

    signateur = SignateurTransactionSimple(clecert)
    formatteur = FormatteurMessageMilleGrilles(idmg, signateur, ca)

    return signateur, formatteur, ca


def load_formatter_core() -> (SignateurTransactionSimple, FormatteurMessageMilleGrilles):
    clecert = CleCertificat.from_files(PATH_CORE_CLE, PATH_CORE_CERT)
    enveloppe = clecert.enveloppe
    idmg = enveloppe.idmg

    ca = EnveloppeCertificat.from_file(PATH_CA_CERT)

    signateur = SignateurTransactionSimple(clecert)
    formatteur = FormatteurMessageMilleGrilles(idmg, signateur, ca)

    return signateur, formatteur, ca


async def authenticate_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    url_get_files = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/files'
    url_logout = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/logout'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()
            json_response = await r.json()
            print("authenticate_1 JSON response: %s\nCookies %s" % (json_response, r.cookies))

        async with session.get(url_get_files) as r:
            r.raise_for_status()
            print("File list")
            while True:
                line = await r.content.readline()
                if not line:
                    break
                line = line.decode('utf-8').strip()
                print(line)

        async with session.get(url_logout) as r:
            r.raise_for_status()

        async with session.get(url_get_files) as r:
            if r.status != 401:
                raise Exception('Should have been 401')

    pass


async def put_file_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    path_file = pathlib.Path('/tmp/zSEfXUAnURyGGW3smduho8dyTo3UHBMo4esqfxA46Wr1mTpgJdQK3pLTcBKwmCP4SFo1GE25b29SpQMYpfnJQwWF1qS6Fs')

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        with open(path_file, 'rb') as fp:
            url_put_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/files/{path_file.name}'
            async with session.put(url_put_file, data=fp) as r:
                r.raise_for_status()


async def get_file_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    path_file = pathlib.Path(
        '/tmp/zSEfXUA2vPzkD1cpCz7o8FnChm5a4EtxfaEHwn4gKy9e5WqaLe4d7gNY6PnEcj4CWdvfeLokTPLkSPPZaYmuxw7QrXgE2f')

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message,
                                                           'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_get_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/files/{path_file.name}'
        with open(f'/tmp/{path_file.name}.new', 'wb') as output:
            async with session.get(url_get_file) as r:
                r.raise_for_status()
                print(r.headers)
                async for chunk in r.content.iter_chunked(64*1024):
                    output.write(chunk)


async def get_usage(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    url_get_usage = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/usage'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()
            json_response = await r.json()
            print("authenticate_1 JSON response: %s\nCookies %s" % (json_response, r.cookies))

        async with session.get(url_get_usage) as r:
            r.raise_for_status()
            print("Usage: %s" % await r.json())


async def delete_file_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    path_file = pathlib.Path('/tmp/zSEfXUA2vPzkD1cpCz7o8FnChm5a4EtxfaEHwn4gKy9e5WqaLe4d7gNY6PnEcj4CWdvfeLokTPLkSPPZaYmuxw7QrXgE2f')

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        with open(path_file, 'rb') as fp:
            url_delete_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/files/{path_file.name}'
            async with session.delete(url_delete_file, data=fp) as r:
                r.raise_for_status()


async def put_file_parts_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    path_file = pathlib.Path('/tmp/zSEfXUF4D5KiY9KxWFikrc4DzBxUMx5VjWCs3DjZhzNV6Ki6WLa2GrZakfUdYZCCsx4ea7HTXDdhsPzVeQmqrXx3wfRpR1')

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        with open(path_file, 'rb') as fp:
            url_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/files/{path_file.name}'
            stat = path_file.stat()
            updload_state = UploadState(path_file.name, fp, stat.st_size)
            await file_upload_parts(session, url_file, updload_state, batch_size=4096)


async def put_backup_file_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    path_file = pathlib.Path('/tmp/backup/CoreCatalogues/CoreCatalogues_20241020132001144Z_C_yF3BcpiHE4gU.mgbak')

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        with open(path_file, 'rb') as fp:
            url_put_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/CoreCatalogues/concatene/yF3BcpiHE4gU/{path_file.name}'
            async with session.put(url_put_file, data=fp) as r:
                r.raise_for_status()


async def get_backup_domains_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_put_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/domaines'
        async with session.get(url_put_file) as r:
            r.raise_for_status()
            domains = await r.json()
            print("Domains\n%s" % domains)


async def get_backup_archives_versions_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_put_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/CoreCatalogues/archives'
        async with session.get(url_put_file) as r:
            r.raise_for_status()
            domains = await r.json()
            print("Domains\n%s" % domains)


async def get_backup_archives_list_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_put_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/CoreCatalogues/archives/yF3BcpiHE4gU'
        async with session.get(url_put_file) as r:
            r.raise_for_status()
            domains = await r.text()
            print("Domains\n------\n%s\n------" % domains)


async def get_backup_file_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_get_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/CoreCatalogues/archives/yF3BcpiHE4gU/CoreCatalogues_20241020132001144Z_C_yF3BcpiHE4gU.mgbak'
        async with session.get(url_get_file) as r:
            r.raise_for_status()
            with open('/tmp/out.mgbak', 'wb') as output:
                async for chunk in r.content.iter_chunked(64*1024):
                    output.write(chunk)


async def get_backup_tarfile_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
    ca_pem = ca.certificat_pem
    signed_message['millegrille'] = ca_pem

    async with aiohttp.ClientSession() as session:
        async with session.post(url_authenticate, json=signed_message) as r:
            r.raise_for_status()

        url_get_file = f'https://thinkcentre1.maple.maceroc.com:3022/filehost/backup_v2/tar/CoreCatalogues'
        async with session.get(url_get_file) as r:
            r.raise_for_status()
            with open('/tmp/out.tar', 'wb') as output:
                async for chunk in r.content.iter_chunked(64*1024):
                    output.write(chunk)


async def main():
    # Create message signing resource
    signateur, formatteur, ca = load_formatter_fichiers()

    # await authenticate_1(formatteur, ca)
    await put_file_1(formatteur, ca)
    # await get_file_1(formatteur, ca)
    # await get_usage(formatteur, ca)
    # await delete_file_1(formatteur, ca)

    signateur_core, formatteur_core, ca = load_formatter_core()
    # await put_backup_file_1(formatteur_core, ca)
    # await get_backup_domains_1(formatteur_core, ca)
    # await get_backup_archives_versions_1(formatteur_core, ca)
    # await get_backup_archives_list_1(formatteur_core, ca)
    # await get_backup_file_1(formatteur_core, ca)
    # await get_backup_tarfile_1(formatteur_core, ca)


if __name__ == '__main__':
    asyncio.run(main())
