from unittest import mock

from polytope_server.common.staging.s3_staging import S3Staging


class DummyMinioClient:
    def __init__(self) -> None:
        self._region = None

    def make_bucket(self, bucket, region):
        return "Dummy make bucket"

    def set_bucket_policy(self, bucket, policy):
        return "Dummy set bucket policy"


class Test:
    @mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
    def test_s3_staging_secure_false(self, mock_minio: mock.Mock):
        mock_minio.return_value = DummyMinioClient()
        s3Staging = S3Staging(config={"secure": False})

        self.verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)

    @mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
    def test_s3_staging_secure_any_value_false(self, mock_minio: mock.Mock):
        mock_minio.return_value = DummyMinioClient()
        s3Staging = S3Staging(config={"secure": "sdafsdfs"})

        self.verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)

    @mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
    def test_s3_staging_secure_default(self, mock_minio: mock.Mock):
        mock_minio.return_value = DummyMinioClient()
        s3Staging = S3Staging(config={})

        self.verify_secure_flag_and_internal_url(mock_minio, s3Staging, False)

    @mock.patch("polytope_server.common.staging.s3_staging.Minio", autospec=True)
    def test_s3_staging_secure_true(self, mock_minio: mock.Mock):
        mock_minio.return_value = DummyMinioClient()
        s3Staging = S3Staging(config={"secure": True})

        self.verify_secure_flag_and_internal_url(mock_minio, s3Staging, True)

    def verify_secure_flag_and_internal_url(self, mock_minio: mock.Mock, s3Staging: S3Staging, secure: bool):
        mock_minio.assert_called_once()
        _, kwargs = mock_minio.call_args
        assert kwargs["secure"] == secure
        assert s3Staging.get_internal_url("test").startswith("https" if secure else "http")
