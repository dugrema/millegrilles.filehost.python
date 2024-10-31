import asyncio
import socketio
import logging

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles

PATH_FICHIERS_CERT = '/var/opt/millegrilles/secrets/pki.fichiers.cert'
PATH_FICHIERS_CLE = '/var/opt/millegrilles/secrets/pki.fichiers.cle'
PATH_CORE_CERT = '/var/opt/millegrilles/secrets/pki.filecontroler.cert'
PATH_CORE_CLE = '/var/opt/millegrilles/secrets/pki.filecontroler.cle'
PATH_CA_CERT = '/var/opt/millegrilles/configuration/pki.millegrille.cert'

LOGGER = logging.getLogger(__name__)

def load_formatter_filecontroler() -> (SignateurTransactionSimple, FormatteurMessageMilleGrilles):
    clecert = CleCertificat.from_files(PATH_CORE_CLE, PATH_CORE_CERT)
    enveloppe = clecert.enveloppe
    idmg = enveloppe.idmg

    ca = EnveloppeCertificat.from_file(PATH_CA_CERT)

    signateur = SignateurTransactionSimple(clecert)
    formatteur = FormatteurMessageMilleGrilles(idmg, signateur, ca)

    return signateur, formatteur, ca


async def connect(formatteur: FormatteurMessageMilleGrilles, ca: EnveloppeCertificat):
    async with socketio.AsyncSimpleClient() as sio:
        auth_message = {'value': 'A message to authenticate'}
        auth_message, message_id = formatteur.signer_message(Constantes.KIND_COMMANDE, auth_message, 'filehost', action='authenticate')
        auth_message['millegrille'] = ca.certificat_pem
        await sio.connect('https://thinkcentre1.maple.maceroc.com:3022', socketio_path='/filehost/socket.io', auth=auth_message)

        await asyncio.sleep(1)
        pass


async def main():
    signateur, formatteur, ca = load_formatter_filecontroler()
    await connect(formatteur, ca)


if __name__ == '__main__':
    asyncio.run(main())
