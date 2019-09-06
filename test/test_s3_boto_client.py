import pytest
from loguru import logger
from datetime import datetime
from s3pcmd import S3BotoClient
import boto3
from botocore.stub import Stubber
from moto import mock_s3

BUCKET = "FOO-TEST-BUCKET"
SECOND_BUCKET = "BAR-TEST-BUCKET"

def create_test_bucket_and_keys(n: int, s3_bucket:str, s3_prefix: str, content:str = ''):
    res = boto3.resource('s3')
    res.create_bucket(Bucket=s3_bucket)
    for key in ["{prefix}/key{idx:03}".format(prefix=s3_prefix, idx=idx) for idx in range(n) ]:
        object = res.Object(s3_bucket, key)
        object.put(Body=content.encode('utf-8'))

@pytest.fixture() 
@mock_s3 
def moto_boto(): 
    res = boto3.resource('s3') 
    res.create_bucket(Bucket=BUCKET) 

@pytest.fixture() 
def invalid_uri():
    return "s4://some-bucket/some-path/somefile"

@pytest.mark.parametrize(
    'uri, bucket, prefix, num_of_objs, list_num, expected_num', [
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        10,
        20,
        10),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        10,
        5,
        5),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        10,
        10,
        10),
    ]
)
@mock_s3
def test_boto_client_ls_objects(uri: str, bucket: str, prefix: str, num_of_objs: int, list_num: int, expected_num: int):
    create_test_bucket_and_keys(num_of_objs, bucket, prefix)
    s3_uri = uri.format(bucket=bucket, prefix=prefix)
    contents = S3BotoClient.ls(s3_uri, list_num)
    assert len(contents) == expected_num


def test_boto_client_ls_exception(invalid_uri: str):
    with pytest.raises(Exception) as e:
        assert S3BotoClient.ls(invalid_uri, 0)
    assert (str(e.value)) == "URI: {s3_uri} is invalid format".format(s3_uri= invalid_uri)

@pytest.mark.parametrize(
    'uri, bucket, prefix, num_of_objs', [
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        0),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        1),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        10),
    ]
)
@mock_s3
def test_boto_client_rmr_objects(uri: str, bucket: str, prefix: str, num_of_objs: int):
    create_test_bucket_and_keys(num_of_objs, bucket, prefix)
    s3_uri = uri.format(bucket=bucket, prefix=prefix)
    S3BotoClient.rm(s3_uri, True)
    contents = S3BotoClient.ls(s3_uri)
    assert len(contents) == 0

@pytest.mark.parametrize(
    'uri, bucket, prefix, file_name, content', [
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        "key000",
        ""),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        "key001",
        '{"content": "test"}')
    ]
)
@mock_s3
def test_boto_client_add_file_objects(uri: str, bucket: str, prefix: str, file_name: str, content: str):
    create_test_bucket_and_keys(0, bucket, prefix)
    s3_full_path = uri.format(bucket=bucket, prefix=prefix)
    S3BotoClient.add_flag_file(s3_full_path, file_name=file_name, content=content)
    item = S3BotoClient.ls(s3_full_path+file_name)[0]
    assert item is not None
    assert item.size == len(content.encode('utf-8'))
    assert item.key == "{prefix}/{file_name}".format(prefix=prefix, file_name=file_name)

@pytest.mark.parametrize(
    'uri_template, src_bucket, src_prefix, dst_bucket, dst_prefix, num_obj', [
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        BUCKET,
        "foo/test",
        10),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        SECOND_BUCKET,
        "foo/test",
        10)
    ]
)
@mock_s3
def test_boto_client_cp_recursive_objects(uri_template: str, src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str, num_obj: int):
    create_test_bucket_and_keys(num_obj, src_bucket, src_prefix)
    create_test_bucket_and_keys(0, dst_bucket, "") # create a second bucket in-case it doesn't exists
    s3_src_path = uri_template.format(bucket=src_bucket, prefix=src_prefix)
    s3_dst_path = uri_template.format(bucket=dst_bucket, prefix=dst_prefix)
    src_items = S3BotoClient.ls(s3_src_path)
    S3BotoClient.cp(s3_src_path, s3_dst_path)
    dst_items = S3BotoClient.ls(s3_dst_path)
    assert len(src_items) == len(dst_items)
    for (src, dst) in zip(src_items, dst_items):
        assert src.key == dst.key
        assert src.size == dst.size
        assert src.owner == dst.owner
        assert src.etag == dst.etag

@pytest.mark.parametrize(
    'uri_template, src_bucket, src_prefix, dst_bucket, dst_prefix, content', [
        ("s3://{bucket}/{prefix}/key000",
        BUCKET,
        "foo/test",
        BUCKET,
        "foo/test",
        "some data"),
        ("s3://{bucket}/{prefix}/",
        BUCKET,
        "foo/test",
        SECOND_BUCKET,
        "foo/test",
        "different data")
    ]
)
@mock_s3
def test_boto_client_cp_single_object(uri_template: str, src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str, content: int):
    create_test_bucket_and_keys(1, src_bucket, src_prefix, content) # create a single object with key000 as its name
    create_test_bucket_and_keys(0, dst_bucket, "") # create a second bucket in-case it doesn't exists
    s3_src_path = uri_template.format(bucket=src_bucket, prefix=src_prefix)
    s3_dst_path = uri_template.format(bucket=dst_bucket, prefix=dst_prefix)
    src_items = S3BotoClient.ls(s3_src_path)
    S3BotoClient.cp(s3_src_path, s3_dst_path)
    dst_items = S3BotoClient.ls(s3_dst_path)
    assert len(src_items) == len(dst_items)
    for (src, dst) in zip(src_items, dst_items):
        assert src.key == dst.key
        assert src.size == dst.size
        assert src.owner == dst.owner
        assert src.etag == dst.etag


