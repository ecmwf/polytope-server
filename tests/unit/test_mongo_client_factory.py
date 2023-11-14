from unittest import mock

from polytope_server.common import mongo_client_factory


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_without_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("host", "123", username=None, password=None, tls=False)

    _verify(mock_mongo, "host:123", False)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_with_credentials(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("host", "123", username="admin", password="admin", tls=False)

    _verify(mock_mongo, "admin:admin@host:123", False)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_without_credentials_tls(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("host", "123", username=None, password=None, tls=True)

    _verify(mock_mongo, "host:123", True)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_with_credentials_tls(mock_mongo: mock.Mock):
    mongo_client_factory.create_client("host", "123", username="admin", password="admin", tls=True)

    _verify(mock_mongo, "admin:admin@host:123", True)


@mock.patch("polytope_server.common.mongo_client_factory.pymongo.MongoClient", autospec=True)
def test_create_with_tlsCAfile(mock_mongo: mock.Mock):
    mongo_client_factory.create_client(
        "host", "123", username="admin", password="admin", tls=True, tlsCAFile="/test/ca.pem"
    )

    _verify(mock_mongo, "admin:admin@host:123", True, "/test/ca.pem")


def _verify(mock_mongo: mock.Mock, endpoint: str, tls: bool, tlsCAFile=None):
    mock_mongo.assert_called_once()
    args, kwargs = mock_mongo.call_args
    assert args[0] == f"mongodb://{endpoint}"
    assert kwargs["tls"] == tls
    assert kwargs["tlsCAFile"] == tlsCAFile
