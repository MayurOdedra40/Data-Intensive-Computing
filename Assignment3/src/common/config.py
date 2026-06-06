"""SSM Parameter Store helper, shared by every Lambda.

In this project NOTHING is hardcoded: bucket names, table names, and tunable numbers all
live in SSM Parameter Store and are read at runtime. This module is the one place that
talks to SSM, so the handlers stay tiny.
"""
import os

import boto3

# The tutorial's trick: when STAGE=local, talk to MiniStack on localhost; otherwise (real
# AWS) leave endpoint_url=None and boto3 finds the real endpoint. run.sh sets STAGE=local on
# every Lambda, so in this project we always hit MiniStack.
_ENDPOINT = "http://localhost:4566" if os.getenv("STAGE") == "local" else None

# Build the client once at import time and reuse it. AWS keeps a Lambda "warm" between
# invocations, so module-level objects are reused -- cheaper than rebuilding one per call.
_ssm = boto3.client("ssm", endpoint_url=_ENDPOINT)


def get(name: str) -> str:
    """Return one SSM String parameter, e.g. get('/dic-a3/buckets/ingest') -> 'reviews-ingest'."""
    return _ssm.get_parameter(Name=name)["Parameter"]["Value"]


def get_int(name: str) -> int:
    """Same as get(), but cast to int. (SSM stores everything as strings!)"""
    return int(get(name))


def get_float(name: str) -> float:
    """Same as get(), but cast to float."""
    return float(get(name))
