import logging

from polytope_server.common.user import User
from polytope_server.frontend.flask_handler import FlaskHandler


class FakeAuth:
    def authenticate(self, auth_header: str) -> User:
        return User(username="alice", realm="ecmwf")


class MissingRequestStore:
    def get_request(self, request_id):
        return None


class ExplodingRequestStore:
    def get_requests(self, **kwargs):
        raise RuntimeError("boom")


def test_http_exception_logs_structured_status_and_error_code(caplog):
    app = FlaskHandler().create_handler(
        request_store=MissingRequestStore(),
        auth=FakeAuth(),
        staging=object(),
        collections={},
        proxy_support=False,
    )

    caplog.set_level(logging.ERROR)

    response = app.test_client().get("/api/v1/requests/missing", headers={"Authorization": "Bearer token"})

    assert response.status_code == 404
    record = next(r for r in caplog.records if r.getMessage().startswith("HTTP error:"))
    assert record.__dict__["http.status"] == 404
    assert record.__dict__["error.code"] == "NotFound"


def test_unexpected_exception_logs_structured_status_and_error_code(caplog):
    app = FlaskHandler().create_handler(
        request_store=ExplodingRequestStore(),
        auth=FakeAuth(),
        staging=object(),
        collections={},
        proxy_support=False,
    )

    caplog.set_level(logging.ERROR)

    response = app.test_client().get("/api/v1/user", headers={"Authorization": "Bearer token"})

    assert response.status_code == 500
    record = next(r for r in caplog.records if r.getMessage().startswith("Unexpected error:"))
    assert record.__dict__["http.status"] == 500
    assert record.__dict__["error.code"] == "RuntimeError"
