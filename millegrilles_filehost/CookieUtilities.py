import base64
import datetime
import json
import jwt

from aiohttp import web
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey, Ed25519PrivateKey
from nacl import secret

from millegrilles_filehost import Constants
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


class Cookie:

    def __init__(self, values: dict):
        self.__values = values
        self.idmg = values['idmg']
        self.expiration = values['expiration']
        self.raise_if_expired()

    def get(self, key: str):
        return self.__values.get(key)

    def raise_if_expired(self):
        if datetime.datetime.now().timestamp() > self.expiration:
            raise CookieExpired()


class CookieExpired(Exception):
    pass


def generate_cookie(secret_cookie_key: bytes, response: web.Response, idmg: str, user_id: Optional[str], roles: Optional[list[str]],
                    exchanges: Optional[list[str]], domaines: Optional[list[str]], delegation_globale: Optional[str]):
    duration = datetime.timedelta(hours=1)
    expiration = datetime.datetime.now() + duration
    expiration_epoch = int(expiration.timestamp())
    max_age = duration.seconds
    cookie_session = {'expiration': expiration_epoch, 'idmg': idmg}
    if user_id:
        cookie_session['user_id'] = user_id
    if roles:
        cookie_session['roles'] = roles
    if exchanges:
        cookie_session['exchanges'] = exchanges
    if domaines:
        cookie_session['domaines'] = domaines
    if delegation_globale:
        cookie_session['delegation_globale'] = delegation_globale
    cookie_bytes = json.dumps(cookie_session).encode('utf-8')

    box = secret.SecretBox(secret_cookie_key)
    cookie_encrypted = box.encrypt(cookie_bytes)
    cookie_b64 = base64.b64encode(cookie_encrypted).decode('utf-8')

    response.set_cookie(Constants.CONST_SESSION_COOKIE_NAME, cookie_b64,
                        max_age=max_age, httponly=True, secure=True, samesite='None')


def decrypt_cookie(secret_cookie_key: bytes, cookie: str) -> Cookie:
    cookie_bytes = base64.b64decode(cookie)
    box = secret.SecretBox(secret_cookie_key)
    cookie_decoded_bytes = box.decrypt(cookie_bytes)
    values = json.loads(cookie_decoded_bytes)
    return Cookie(values)


def generate_jwt(private_key: Ed25519PrivateKey, idmg: str, user_id: Optional[str], roles: Optional[list[str]],
                 exchanges: Optional[list[str]], domaines: Optional[list[str]], delegation_globale: Optional[str]) -> str:

    duration = datetime.timedelta(hours=1)
    expiration = datetime.datetime.now() + duration
    expiration_epoch = int(expiration.timestamp())
    cookie_session = {'idmg': idmg}
    if user_id:
        cookie_session['user_id'] = user_id
    if roles:
        cookie_session['roles'] = roles
    if exchanges:
        cookie_session['exchanges'] = exchanges
    if domaines:
        cookie_session['domaines'] = domaines
    if delegation_globale:
        cookie_session['delegation_globale'] = delegation_globale

    headers = {
        'exp': expiration_epoch,
    }
    jwt_str = jwt.encode(cookie_session, private_key, algorithm="EdDSA", headers=headers)
    return jwt_str


def get_headers_jwt(jwt_value: str):
    return jwt.get_unverified_header(jwt_value)


def verify_jwt(public_key: Ed25519PublicKey, jwt_value: str):
    """
    :param public_key:
    :param jwt_value:
    :raises: jwt.exceptions.InvalidSignatureError
    :return:
    """
    headers = jwt.get_unverified_header(jwt_value)
    expiration = headers['exp']
    values = jwt.decode(jwt_value, public_key, algorithms=["EdDSA"])
    # values = json.loads(values)
    values['expiration'] = expiration
    return Cookie(values)

