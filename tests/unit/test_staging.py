import logging
import os
from unittest import mock

import pytest
from moto import mock_aws

import polytope_server.common.config as polytope_config
from polytope_server.common.staging import staging


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    values = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "MOTO_S3_CUSTOM_ENDPOINTS": "http://localhost:8088",
    }
    with mock.patch.dict(os.environ, values):
        yield


@pytest.fixture(scope="function")
def s3_config(aws_credentials):
    with mock_aws():
        yield {
            "s3": {
                "bucket": "test",
                "host": "http://localhost",
                "port": "8088",
                "access_key": "testing",
                "secret_key": "testing",
            }
        }


def test_create_with_presigned_url(s3_config):
    s3_config["s3"]["use_presigned_url"] = True
    s3_staging = staging.create_staging(s3_config)
    data = [b"test data"]
    name = "mydata"

    url = s3_staging.create(name, data, "text/html")
    assert "AWSAccessKeyId" in url
    assert "http://localhost:8088/test/" + name + ".bin" in url


def test_create(s3_config):
    s3_config["s3"]["url"] = "http://localhost:8088"
    s3_staging = staging.create_staging(s3_config)
    data = [b"test data"]
    name = "mydata"

    url = s3_staging.create(name, data, "text/html")
    assert "http://localhost:8088/test/" + name + ".bin" == url
