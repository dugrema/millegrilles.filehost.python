import asyncio
import aiohttp

import logging

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat

PATH_CORE_CERT = '/var/opt/millegrilles/secrets/pki.fichiers.cert'
PATH_CORE_CLE = '/var/opt/millegrilles/secrets/pki.fichiers.cle'
PATH_CORE_CA = '/var/opt/millegrilles/configuration/pki.millegrille.cert'

logger = logging.getLogger(__name__)


def load_formatter() -> (SignateurTransactionSimple, FormatteurMessageMilleGrilles):
    clecert = CleCertificat.from_files(PATH_CORE_CLE, PATH_CORE_CERT)
    enveloppe = clecert.enveloppe
    idmg = enveloppe.idmg

    ca = EnveloppeCertificat.from_file(PATH_CORE_CA)

    signateur = SignateurTransactionSimple(clecert)
    formatteur = FormatteurMessageMilleGrilles(idmg, signateur, ca)

    return signateur, formatteur, ca

async def authenticate_1(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    url_authenticate = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/authenticate'
    url_get_files = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/files'
    url_logout = 'https://thinkcentre1.maple.maceroc.com:3022/filehost/logout'

    auth_message = dict()
    signed_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, action='authenticate')
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


async def main():
    # Create message signing resource
    signateur, formatteur, ca = load_formatter()

    await authenticate_1(formatteur, ca)


if __name__ == '__main__':
    asyncio.run(main())
