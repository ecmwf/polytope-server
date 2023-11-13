from unittest import mock

from ...polytope_server.common.staging.s3_staging import S3Staging


@mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
def test_s3_staging_secure_false(mock_minio: mock.Mock):
    s3Staging = S3Staging(config={"secure": False})

    verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)


@mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
def test_s3_staging_secure_any_value_false(mock_minio: mock.Mock):
    s3Staging = S3Staging(config={"secure": "sdafsdfs"})

    verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)


@mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
def test_s3_staging_secure_default(mock_minio: mock.Mock):
    s3Staging = S3Staging(config={})

    verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)


@mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
def test_s3_staging_secure_true(mock_minio: mock.Mock):
    s3Staging = S3Staging(config={"secure": True})

    verify_secure_flag_and_internal_url(mock_minio, s3Staging, True)


def verify_secure_flag_and_internal_url(mock_minio: mock.Mock, s3Staging: S3Staging, secure: bool):
    mock_minio.assert_called_once()
    _, kwargs = mock_minio.call_args
    assert kwargs["secure"] == secure
    assert s3Staging.get_internal_url("test").startswith("https" if secure else "http")
