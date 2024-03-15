import typing

import pymongo


def create_client(
    uri: str,
    username: typing.Optional[str] = None,
    password: typing.Optional[str] = None,
) -> pymongo.MongoClient:
    if username and password:
        return pymongo.MongoClient(host=uri, journal=True, connect=False, username=username, password=password)
    else:
        return pymongo.MongoClient(host=uri, journal=True, connect=False)
