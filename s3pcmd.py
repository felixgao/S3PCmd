#!/usr/bin/env python

#
# Copyright 2012-2018 Intuit, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
S3P command line tool.
"""

import errno
import enum
import fnmatch
import hashlib
import json
import multiprocessing
import optparse
import os
import random
import re
import shlex
import socket
import sys
import io
import itertools as it
import threading
import time
import traceback
import types
from collections import namedtuple
from functools import lru_cache


import pytz
from datetime import datetime, timedelta
from loguru import logger
from typing import Type, Iterator, List, NamedTuple, Dict, Union, IO
from boto3_type_annotations.s3 import Client, ServiceResource, Bucket
from boto3_type_annotations.s3.waiter import BucketExists
from boto3_type_annotations.s3.paginator import ListObjectsV2


class S3Path(object):
    """
        S3Path object's path is assumed to be a prefix key if it ends with / 
        otherwise it is assumed to be an object in S3
    """

    S3PATH_PATTERN = re.compile(r"(s3[n]?://)([^/]+)[/]?(.*)")

    def __init__(self, uri: str):
        self.proto = "s3"
        try:
            _, self.bucket, self.path = S3Path.S3PATH_PATTERN.match(uri).groups()
            # normalizing the path to remove multiple / in prefix if exists
            self.path = S3Path.normalize_path(self.path)
        except:
            raise RuntimeError("Invliad S3 URI: %{uri}".format(uri=uri))
        self.s3_bucket: Bucket = S3BotoClient.get_bucket(self.bucket)

    def __str__(self) -> str:
        return "{proto}://{fullpath}".format(
            proto=self.proto, fullpath="/".join([self.bucket, self.path])
        )

    @classmethod
    def normalize_path(cls, uri:str) -> str:
        # return uri.replace(r"/\/\/+/g", "/")
        return re.sub(r"\/\/+", "/", uri)

    def __eq__(self, other: "S3Path") -> bool:
        return self.path == other.path and self.bucket == other.bucket

    @lru_cache(maxsize=1)
    def is_file(self) -> bool:
        return not self.path.endswith("/")

    def put(self, postfix: str, body: Union[bytes, IO] = b"") -> None:
        self.s3_bucket.put_object(
            # prefix already contains trailing / 
            Key=S3Path.normalize_path("{prefix}/{postfix}".format(prefix=self.path, postfix=postfix)),
            ACL="bucket-owner-full-control",
            Body=body,
        )

    def ls(self, page_size: int = 100, max_items: int = 1_000_000) -> Iterator[Dict]:
        s3 = S3BotoClient.get_client()
        paginator: ListObjectsV2 = s3.get_paginator("list_objects_v2")
        operation_parameters = {"Bucket": self.bucket, "Prefix": self.path}
        for page_iterator in paginator.paginate(
            **operation_parameters,
            PaginationConfig={"MaxItems": max_items, "PageSize": page_size},
        ):
            try:
                contents = page_iterator["Contents"]
            except KeyError:
                return
            for obj in contents:
                # key = obj["Key"]
                yield obj

    def rmr(self) -> None:
        """
        this is a recursive remove of objects that matches the prefix path in the bucket.
        Use it with caution.
      """
        self.s3_bucket.objects.filter(Prefix=self.path).delete()

    def cp(self, dest_path: "S3Path", excludes: List[str] = ["_SUCCESS"]) -> None:
        """
        Copy objects from current S3Path object to another S3Path object. 
        Assume both bucket exists already and the source.
        After copying the bucket owner have full control of the copied data.
        The excludes must be a postfix string
      """
        # TODO: push any key starts with _ to a queue before the rest of the objects are copied
        for obj in self.ls():
            if any([obj["Key"].endswith(excluded) for excluded in excludes]):
                continue
            key = obj["Key"]
            from_resource = {"Bucket": self.bucket, "Key": key}
            new_key = dest_path.path + key.replace(self.path, "")
            logger.debug(
                "creating new object s3://{bucket}/{prefix} from s3://{old_bucket}/{old_prefix}".format(
                    bucket=dest_path.bucket,
                    prefix=new_key,
                    old_bucket=self.bucket,
                    old_prefix=key,
                )
            )
            dest_path.s3_bucket.copy(
                from_resource,
                Key=new_key,
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

    def mv(self, to_path: "S3Path", excludes: List[str] = []) -> None:
        """
        Copy objects from current S3Path object to another S3Path object.
        Then delete everything in the current S3Path object
        Assume both bucket exists already.
        After copying the bucket owner have full control of the copied data.
        WARNING: This operation is not atomic and idempotent.  Use it at your 
        own risk
      """
        self.cp(to_path, excludes=excludes)
        self.rmr()

    @staticmethod
    def is_valid(uri: str) -> bool:
        """Check if given uri is a valid S3 URL"""
        return S3Path.S3PATH_PATTERN.match(uri) != None


@enum.unique
class DateType(enum.Enum):
    DATEID = enum.auto()
    DATETIMEID = enum.auto()


@enum.unique
class DateOp(enum.Enum):
    NONE = enum.auto()
    PLUS = enum.auto()
    MINUS = enum.auto()

    @staticmethod
    def from_str(label):
        if label == "+":
            return DateOp.PLUS
        elif label == "-":
            return DateOp.MINUS
        else:
            return DateOp.NONE


class S3DateParam(NamedTuple):
    idx: int
    dt: DateType
    sign: DateOp
    value: int = 0  # default to 0, so noop


class S3DateValue(NamedTuple):
    idx: int
    token: str
    value: str


class S3ListContent(NamedTuple):
    key: str
    size: int
    owner: str
    etag: str = ""


# This represent a str of s3://bucket/p/a/t/h/{DATEID}/data
# where DATEID is a marker to replace value with current/assumed date
# additional arithmetics maybe performed on the DATEID by using the following
# {DATEID-1} for previoues day of the current DATEID
# {DATEID+1} for next day of the current DATEID
# {DATETIMEID-1} for previous day of the current DATETIMEID
# {DATETIMEID+1} for next day of the current DATETIMEID
class S3DatePath(S3Path):
    PARAM_PATTERN = re.compile(r".*{(DATE(TIME)?ID)(([-+])(\d+))?}.*")
    DATE_STR = "%Y-%m-%d"
    DATETIME_STR = "%Y-%m-%d_%H-%M-%S"

    def __init__(self, uri: str):
        super(S3DatePath, self).__init__(uri)
        self._path_lst = [p for p in self.path.split("/") if p.strip() != ""]
        self._params = self._parse_params(self._path_lst)

    def _parse_params(self, path_lst: List[str]) -> List[S3DateParam]:
        params = []
        for idx, val in enumerate(path_lst):
            # TODO: this heuristic might not work for complicated string
            if ("{DATEID" in val or "{DATETIMEID" in val) and "}" in val:
                dtype, _, _, sign, value = S3DatePath.PARAM_PATTERN.match(val).groups()
                dtype, sign = DateType[dtype], DateOp.from_str(sign)
                value = int(value) if value else 0
                params.append(S3DateParam._make([idx, dtype, sign, value]))
        return params

    def resolve_dateid(self, dt: datetime) -> str:
        resolved_lst = [
            S3DateValue._make(self._extract_value(param, dt)) for param in self._params
        ]
        for el in resolved_lst:
            self._path_lst[el.idx] = self._path_lst[el.idx].replace(el.token, el.value)
        return "{proto}://{fullpath}".format(
            proto=self.proto, fullpath="/".join([self.bucket, "/".join(self._path_lst)])
        )

    def _compute_offset(self, dt: datetime, op: DateOp, value: int) -> datetime:
        td = timedelta(days=value)
        return dt - td if op == DateOp.MINUS else dt + td

    def _reconstruct_match_token(self, param: Type[S3DateParam]) -> str:
        lst = [param.dt.name]
        if param.sign in (DateOp.PLUS, DateOp.MINUS):
            lst.append("+" if param.sign == DateOp.PLUS else "-")
            lst.append(str(param.value))
        return "{{{token}}}".format(token="".join(lst))

    def _extract_value(self, param: Type[S3DateParam], dt: datetime) -> List:
        ts = (
            dt
            if param.sign == DateOp.NONE
            else self._compute_offset(dt, param.sign, param.value)
        )
        ts_str = (
            ts.strftime(S3DatePath.DATE_STR)
            if param.dt == DateType.DATEID
            else ts.strftime(S3DatePath.DATETIME_STR)
        )
        token = self._reconstruct_match_token(param)
        return [param.idx, token, ts_str]


def log_calls(func):
    """Decorator to log debug function calls."""

    def wrapper(*args, **kargs):
        callStr = "%s(%s)" % (
            func.__name__,
            ", ".join(
                [repr(p) for p in args]
                + ["%s=%s" % (k, repr(v)) for (k, v) in list(kargs.items())]
            ),
        )
        logger.debug(f">> {callStr}")
        ret = func(*args, **kargs)
        logger.debug(f"<< {callStr}: {ret}")
        return ret

    return wrapper


class S3BotoClient(object):
    """
    S3BotoClient is a pure client that only deals with S3 paths from source and destination.
    Currently supported operatons are
    1. rmr s3 data using its full path.
    2. cp from one S3 location to another S3 location
    3. mv from one S3 location to another S3 location
    4. add empty meta data(flag file) to a S3 location
  """

    # Encapsulate boto3 interface intercept all API calls.
    boto3 = __import__("boto3")  # version >= 1.3.1
    botocore = __import__("botocore")
    SUCCESS_FILE = "_SUCCESS"

    # Exported exceptions.
    BotoError = boto3.exceptions.Boto3Error
    ClientError = botocore.exceptions.ClientError
    NoCredentialsError = botocore.exceptions.NoCredentialsError

    class InvalidS3PathError(BotoError):
        pass

    @classmethod
    @log_calls
    def rm(cls, s3_uri: str, recursive=False) -> None:
        if not S3Path.is_valid(s3_uri):
            raise cls.InvalidS3PathError(
                "URI: {s3_uri} is invalid format".format(s3_uri=s3_uri)
            )
        s3_path = S3Path(s3_uri)
        if s3_path.is_file():
            s3_path.rmr()
        else:
            if not recursive:
                raise cls.ClientError(
                    "Attempt to delete {uri} which is not a file object.\nPlease use recursive flag and try again".format(
                        uri=s3_uri
                    )
                )
            else:
                s3_path.rmr()

    @classmethod
    @log_calls
    def ls(cls, s3_uri: str, limit=1_000) -> List[S3ListContent]:
        """ Performs cases in-sensitive listing of keys under the path """
        if not S3Path.is_valid(s3_uri):
            raise cls.InvalidS3PathError(
                "URI: {s3_uri} is invalid format".format(s3_uri=s3_uri)
            )
        s3_path = S3Path(s3_uri)
        return list(
            it.islice(
                (
                    S3ListContent._make(
                        [
                            obj["Key"],
                            obj.get("Size", 0),
                            obj.get("Owner", {}).get("DisplayName", ""),
                            obj.get("ETag", ''),
                        ]
                    )
                    for obj in s3_path.ls()
                ),
                limit,
            )
        )

    @classmethod
    @log_calls
    def cp(cls, s3_src_uri: str, s3_dst_uri: str) -> None:
        """ cp will copy data recursively if it is a prefix """
        if S3Path.is_valid(s3_src_uri) and S3Path.is_valid(s3_dst_uri):
            s3_src_path, s3_dst_path = S3Path(s3_src_uri), S3Path(s3_dst_uri) 
            if s3_src_path != s3_dst_path: # if you are trying to copy the data from the same src/dst, noop
                s3_src_path.cp(s3_dst_path)
        else:
            raise cls.InvalidS3PathError(
                f"URI: {s3_src_uri} or {s3_dst_uri} is invalid format"
            )

    @classmethod
    @lru_cache(maxsize=1)
    def get_client(cls) -> Client:
        return cls.boto3.client("s3")

    @classmethod
    @lru_cache(maxsize=1)
    def _s3_resource(cls, region: str = "us-west-2") -> ServiceResource:
        return cls.boto3.resource("s3", region_name=region)

    @classmethod
    def get_bucket(cls, bucket_name: str) -> Bucket:
        return cls._s3_resource().Bucket(bucket_name)

    @classmethod
    @log_calls
    def add_flag_file(
        cls, s3_uri: str, file_name: str = "_SUCCESS", content: str = ""
    ) -> None:
        if not S3Path.is_valid(s3_uri):
            raise cls.InvalidS3PathError(
                "URI: {s3_uri} is invalid format".format(s3_uri=s3_uri)
            )
        s3_path = S3Path(s3_uri)
        s3_path.put(file_name, content.encode("utf-8"))



def main():
    pass


if __name__ == "__main__":
    main()
