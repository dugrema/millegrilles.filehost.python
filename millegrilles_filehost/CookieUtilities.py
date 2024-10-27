import base64
import datetime
import json

from aiohttp import web
from typing import Optional
from nacl import secret

from millegrilles_filehost import Constants

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


def generate_cookie(secret_cookie_key: bytes, response: web.Response, idmg: str, user_id: Optional[str], roles: Optional[str],
                    exchanges: Optional[str]):
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
    cookie_bytes = json.dumps(cookie_session).encode('utf-8')

    box = secret.SecretBox(secret_cookie_key)
    cookie_encrypted = box.encrypt(cookie_bytes)
    cookie_b64 = base64.b64encode(cookie_encrypted).decode('utf-8')

    response.set_cookie(Constants.CONST_SESSION_COOKIE_NAME, cookie_b64,
                        max_age=max_age, httponly=True, secure=True)

def decrypt_cookie(secret_cookie_key: bytes, cookie: str) -> Cookie:
    cookie_bytes = base64.b64decode(cookie)
    box = secret.SecretBox(secret_cookie_key)
    cookie_decoded_bytes = box.decrypt(cookie_bytes)
    values = json.loads(cookie_decoded_bytes)

    return Cookie(values)
