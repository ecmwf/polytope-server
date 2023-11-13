import pymongo
import urllib.parse
import typing


def create_client(
    host: str,
    port: str,
    username: typing.Optional[str] = None,
    password: typing.Optional[str] = None,
    tls: bool = False,
) -> pymongo.MongoClient:
    endpoint = f"{host}:{port}"

    if username and password:
        encoded_username = urllib.parse.quote_plus(f"{username}")
        encoded_password = urllib.parse.quote_plus(f"{password}")
        endpoint = f"{encoded_username}:{encoded_password}@{endpoint}"

    return pymongo.MongoClient(endpoint, journal=True, connect=False, tls=tls)
