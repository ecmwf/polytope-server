import typing
import urllib.parse

import pymongo


def create_client(
    host: str,
    port: str,
    username: typing.Optional[str] = None,
    password: typing.Optional[str] = None,
    tls: bool = False,
    tlsCAFile: typing.Optional[str] = None,
) -> pymongo.MongoClient:
    endpoint = f"mongodb://{host}:{port}"

    if username and password:
        encoded_username = urllib.parse.quote_plus(username)
        encoded_password = urllib.parse.quote_plus(password)
        endpoint = f"mongodb://{encoded_username}:{encoded_password}@{host}:{port}"

    return pymongo.MongoClient(endpoint, journal=True, connect=False, tls=tls, tlsCAFile=tlsCAFile)
