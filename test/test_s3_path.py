import pytest
from loguru import logger
from datetime import datetime
from s3pcmd import S3Path, S3DatePath, S3BotoClient
import boto3
from botocore.stub import Stubber

@pytest.mark.parametrize(
    'uri, expected, valid', [
        ("s3://test-bucket/path/p2/", 
        "s3://test-bucket/path/p2/",
        True),
        ("s3://test-bucket/path/p2/", 
        'Fizz',
        False)
    ]
)
def test_s3path_objects(uri: str, expected: str, valid: bool):
    if valid:
        s3p = S3Path(uri)
        assert True == S3Path.is_valid(uri)
        assert s3p.__str__() == expected
    else:
        s3p = S3Path(uri)
        assert False == valid
        assert s3p.__str__() != expected


@pytest.mark.parametrize(
    'uri, expected', [
        ("s3://test-bucket/path/p2/{DATEID}", 
        "s3://test-bucket/path/p2/2017-11-28"
        ),
        ("s3://test-bucket/path/p2/some_{DATETIMEID-1}", 
        's3://test-bucket/path/p2/some_2017-11-27_23-55-59'
        ),
        ("s3://test-bucket/path/p2/some_{DATETIMEID-1}_postfix", 
        's3://test-bucket/path/p2/some_2017-11-27_23-55-59_postfix'
        ),
        ("s3://test-bucket/path/p2", 
        "s3://test-bucket/path/p2"
        ),
        ("s3://test-bucket//path//////p2", 
        "s3://test-bucket/path/p2"
        ),
    ]
)
def test_s3pathdate_objects(uri: str, expected: str):
    s3p = S3DatePath(uri)
    dt = datetime(2017, 11, 28, 23, 55, 59, 342380)
    assert expected == s3p.resolve_dateid(dt)
