import pytest
from loguru import logger
from datetime import datetime
from s3pcmd import S3Path, S3BotoClient
import boto3
from moto import mock_s3
from typing import Type, List


BUCKET = "FOO-TEST-BUCKET"

def create_test_bucket_and_keys(n: int, s3path: Type[S3Path]):
    res = boto3.resource('s3')
    res.create_bucket(Bucket=s3path.bucket)
    bucket = S3BotoClient.get_bucket(s3path.bucket)
    for key in ["{prefix}/key{idx:03}".format(prefix=s3path.path, idx=idx) for idx in range(n) ]:
        bucket.put_object(Key=key, Body=b'')

def add_excluded_files(s3pth: Type[S3Path], excludes:List[str] = []):
    for exclude in excludes:
        s3pth.put(postfix="{prefix}/{exclude}".format(prefix=s3pth.path, exclude=exclude), body=b'')

@pytest.fixture() 
@mock_s3 
def moto_boto(): 
    res = boto3.resource('s3') 
    res.create_bucket(Bucket=BUCKET) 

@pytest.mark.parametrize(
    'uri, expected', [
        ("s3://{bucket}/prefix/2019-09-01/".format(bucket=BUCKET), 
        2
        ),
        ("s3://{bucket}/prefix/2019-09-01/key000".format(bucket=BUCKET), 
        1
        )
    ]
)
@mock_s3
def test_s3path_ls_objects(uri: str, expected: int):
    res = boto3.resource('s3')
    res.create_bucket(Bucket=BUCKET)
    s3_path = S3Path(uri)
    bucket = S3BotoClient.get_bucket(s3_path.bucket)
    if s3_path.is_file() :
        bucket.put_object(Key=s3_path.path, Body=b'')
        items = sorted([obj for obj in s3_path.ls(page_size=1)], key=lambda obj: obj['Key'], reverse=True)
        assert len(items) == expected
        assert items[0]['Key'] == s3_path.path
    else :
        for key in ["{prefix}/key{idx:03}".format(prefix=s3_path.path, idx=idx) for idx in range(2) ]:
            bucket.put_object(Key=key, Body=b'')
        bucket.put_object(Key="{prefix}/_SUCCESS".format(prefix=s3_path.path))
        items = sorted([obj for obj in s3_path.ls(page_size=1)], key=lambda obj: obj['Key'], reverse=True)
        assert len(items) == expected + 1

@pytest.mark.parametrize(
    'from_path, to_path, file_count, excludes', [
        (
            S3Path("s3://{bucket}/prefix/2019-09-01".format(bucket=BUCKET)), 
            S3Path("s3://{bucket}/new_prefix/2019-09-01".format(bucket=BUCKET)), 
            2,
            []
        ),
         (
            S3Path("s3://{bucket}/prefix/2019-09-01".format(bucket=BUCKET)), 
            S3Path("s3://{bucket}/new_prefix/2019-09-02".format(bucket=BUCKET)), 
            2,
            ["_SUCCESS"]
        )
    ]
)
@mock_s3
def test_s3path_cp_objects(from_path: Type[S3Path], to_path: Type[S3Path], file_count: int, excludes: List):
    create_test_bucket_and_keys(file_count, from_path)
    if len(excludes):
        add_excluded_files(from_path, excludes)
    from_path.cp(to_path, excludes)
    orig_items = list(from_path.ls())
    items = list(to_path.ls())
    assert len(orig_items) == file_count + len(excludes)
    assert len(items) == file_count

@pytest.mark.parametrize(
    'from_path, to_path, file_count, excludes', [
        (
            S3Path("s3://{bucket}/prefix/2019-09-01".format(bucket=BUCKET)), 
            S3Path("s3://{bucket}/new_prefix/2019-09-01".format(bucket=BUCKET)), 
            2,
            []
        ),
         (
            S3Path("s3://{bucket}/prefix/2019-09-01".format(bucket=BUCKET)), 
            S3Path("s3://{bucket}/new_prefix/2019-09-02".format(bucket=BUCKET)), 
            2,
            ["_SUCCESS"]
        )
    ]
)
@mock_s3
def test_s3path_mv_objects(from_path: Type[S3Path], to_path: Type[S3Path], file_count: int, excludes: List):
    create_test_bucket_and_keys(file_count, from_path)
    if len(excludes):
        add_excluded_files(from_path, excludes)
    orig_src_items = list(from_path.ls())
    orig_dst_items = list(to_path.ls())
    from_path.mv(to_path, excludes)
    new_src_items = list(from_path.ls())
    new_dst_items = list(to_path.ls())
    assert len(orig_src_items) == file_count + len(excludes)
    assert len(orig_dst_items) == 0
    assert len(new_dst_items) == file_count
    assert len(new_src_items) == 0

@pytest.mark.parametrize(
    'from_path, file_count', [
        (
            S3Path("s3://{bucket}/prefix/2019-09-01".format(bucket=BUCKET)),
            10 
        )
    ]
)
@mock_s3
def test_s3path_rmr_objects(from_path: Type[S3Path], file_count: int):
    create_test_bucket_and_keys(file_count, from_path)
    orig_items = list(from_path.ls())
    from_path.rmr()
    items = list(from_path.ls())
    assert len(orig_items) == file_count
    assert len(items) == 0

@pytest.mark.parametrize(
    'from_path, postfix, content', [
        (
            S3Path("s3://{bucket}/prefix/2019-09-01/".format(bucket=BUCKET)),
            '_META_',
            '{"JobID": 1, "RunDate": "2019-09-01"}'
        )
    ]
)
@mock_s3
def test_s3path_put_objects(from_path: Type[S3Path], postfix:str, content:str):
    res = boto3.resource('s3')
    res.create_bucket(Bucket=from_path.bucket)
    orig_items = list(from_path.ls())
    from_path.put(postfix, body=content.encode('utf-8'))
    new_items = list(from_path.ls())
    assert len(orig_items) == 0
    assert len(new_items) == 1
    assert new_items[0]['Size'] == len(content.encode('utf-8'))