import datetime
import logging
import pathlib

from aiohttp import web
from cryptography.x509 import ExtensionNotFound
from typing import Optional

from millegrilles_filehost import Constants
from millegrilles_filehost.Context import FileHostContext
from millegrilles_filehost.CookieUtilities import generate_cookie
from millegrilles_messages.messages.ValidateurCertificats import ValidateurCertificatCache, ValidateurCertificat
from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage


class AuthenticationHandler:

    def __init__(self, context: FileHostContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__validator = self.prepare_message_validator()

    def prepare_message_validator(self) -> ValidateurMessage:
        validateur_certificats = ValidateurCertificatCache(None)
        return ValidateurMessage(validateur_certificats)

    async def run(self):
        self.prepare_message_validator()
        while self.__context.stopping is False:
            await self.maintenance()
            await self.__context.wait(30)

    async def maintenance(self):
        self.__logger.debug("Maintenance cycle")

    async def authenticate(self, request: web.Request) -> web.Response :
        auth_message = await request.json()
        try:
            estampille = auth_message['estampille']
            now = datetime.datetime.now().timestamp()
            if now - 45 < estampille < now + 15:
                pass  # Ok, within ~1 minute window
            else:
                self.__logger.warning("Auth message outside the 5 minute window")
                return web.HTTPForbidden()

            enveloppe = await self.__validator.verifier(auth_message, utiliser_idmg_message=True)
            idmg = enveloppe.idmg

            # Ensure that this idmg is hosted here
            path_idmg = pathlib.Path(self.__context.configuration.dir_files, idmg)
            if path_idmg.exists() is False:
                self.__logger.warning('IDMG %s not hosted here' % idmg)
                return web.HTTPForbidden()

        except Exception:
            self.__logger.exception("Error validating auth request")
            return web.HTTPForbidden()
        else:
            # Create a new session, return cookie and JWT (if requested)
            response = {'ok': True, 'idmg': idmg}
            try:
                roles = enveloppe.get_roles
                response['roles'] = roles
            except ExtensionNotFound:
                roles = None
            try:
                exchanges = enveloppe.get_exchanges
                response['exchanges'] = exchanges
            except ExtensionNotFound:
                exchanges = None
            try:
                user_id = enveloppe.get_user_id
                response['user_id'] = user_id
            except ExtensionNotFound:
                user_id = None

            response = web.json_response(response)
            self.generate_cookie(response, idmg, user_id, roles, exchanges)
            return response

    async def logout(self, request: web.Request) -> web.Response :
        response = web.HTTPOk()
        response.del_cookie(Constants.CONST_SESSION_COOKIE_NAME)
        return response

    def generate_cookie(self, response: web.Response, idmg, user_id: Optional[str], roles: Optional[str], exchanges: Optional[str]):
        generate_cookie(self.__context.secret_cookie_key, response, idmg, user_id, roles, exchanges)
