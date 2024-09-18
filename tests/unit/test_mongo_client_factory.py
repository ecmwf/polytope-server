import typing
from unittest import mock

from polytope_server.common import mongo_client_factory


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_without_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("mongodb://host:123")

    _verify(mock_mongo, "mongodb://host:123", None, None)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_without_password_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("mongodb+srv://host:123", username="admin")

    _verify(mock_mongo, "mongodb+srv://host:123", None, None)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_without_username_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("host:123", password="password")

    _verify(mock_mongo, "host:123", None, None)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_with_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("mongodb+srv://host", username="admin", password="est123123")

    _verify(mock_mongo, "mongodb+srv://host", "admin", "est123123")


def _verify(
    mock_mongo: mock.Mock, endpoint: str, username: typing.Optional[str] = None, password: typing.Optional[str] = None
):
    mock_mongo.assert_called_once()
    args, kwargs = mock_mongo.call_args
    assert args[0] == endpoint
    if username:
        assert kwargs["username"] == username
    if password:
        assert kwargs["password"] == password
