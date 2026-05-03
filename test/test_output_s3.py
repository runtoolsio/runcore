"""Tests for S3 output backend (read side)."""
import gzip
import json

import boto3
import pytest
from moto import mock_aws

from runtools.runcore.job import iid
from runtools.runcore.output import OutputLine, OutputStorageConfig
from runtools.runcore.output.s3 import (
    S3OutputBackend,
    create_backend,
    object_key,
    parse_s3_config,
)

BUCKET = "test-runtools-output"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def backend(s3_client):
    return S3OutputBackend(s3_client, BUCKET, prefix="prod")


def _put_jsonl(client, key, lines, *, compressed=True, metadata=None):
    payload = b"".join(
        (json.dumps(line.serialize(), ensure_ascii=False) + "\n").encode("utf-8")
        for line in lines
    )
    if compressed:
        payload = gzip.compress(payload)
    kwargs = dict(Bucket=BUCKET, Key=key, Body=payload, ContentType="application/x-ndjson")
    if compressed:
        kwargs["ContentEncoding"] = "gzip"
    if metadata:
        kwargs["Metadata"] = metadata
    client.put_object(**kwargs)


def test_object_key_layout():
    assert object_key("prod", iid("job1", "run1", 1), compress=True) == "prod/job1/run1__1.jsonl.gz"
    assert object_key("prod", iid("job1", "run1", 1), compress=False) == "prod/job1/run1__1.jsonl"
    assert object_key("", iid("job1", "run1", 2), compress=True) == "job1/run1__2.jsonl.gz"
    assert object_key("/prod/", iid("job1", "run1", 1), compress=True) == "prod/job1/run1__1.jsonl.gz"


def test_read_returns_empty_when_no_object(backend):
    assert backend.read_output(iid("ghost", "run1")) == []


def test_read_wraps_non_404_clienterror_from_head(backend):
    """HEAD failures (permission, network) must wrap into OutputReadError.

    Previously _resolve_key let non-404 ClientErrors propagate raw to the
    caller, bypassing the OutputReadError contract that downstream code
    (MultiSourceOutputReader) relies on for "try next backend" semantics.
    """
    from unittest.mock import patch
    from botocore.exceptions import ClientError
    from runtools.runcore.output import OutputReadError

    err = ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "HeadObject")
    with patch.object(backend._client, "head_object", side_effect=err):
        with pytest.raises(OutputReadError):
            backend.read_output(iid("job1", "run1"))


def test_read_wraps_botocore_transport_errors(backend):
    """Transport errors (BotoCoreError, e.g. EndpointConnectionError) must wrap
    into OutputReadError too — same contract as ClientError. A raw BotoCoreError
    escaping read_output would bypass MultiSourceOutputReader's fallback chain.
    """
    from unittest.mock import patch
    from botocore.exceptions import EndpointConnectionError
    from runtools.runcore.output import OutputReadError

    err = EndpointConnectionError(endpoint_url="https://s3.example/")
    with patch.object(backend._client, "head_object", side_effect=err):
        with pytest.raises(OutputReadError):
            backend.read_output(iid("job1", "run1"))


def test_read_compressed_object(backend, s3_client):
    lines = [OutputLine("hello", 1, source="EXEC"), OutputLine("world", 2, source="EXEC")]
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl.gz", lines, compressed=True)

    result = backend.read_output(iid("job1", "run1"))

    assert [ol.message for ol in result] == ["hello", "world"]


def test_read_uncompressed_object(backend, s3_client):
    lines = [OutputLine("plain", 1)]
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl", lines, compressed=False)

    result = backend.read_output(iid("job1", "run1"))

    assert [ol.message for ol in result] == ["plain"]


def test_read_filters_by_source(backend, s3_client):
    lines = [
        OutputLine("a", 1, source="EXEC"),
        OutputLine("b", 2, source="OTHER"),
        OutputLine("c", 3, source="EXEC"),
    ]
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl.gz", lines)

    result = backend.read_output(iid("job1", "run1"), sources={"EXEC"})

    assert [ol.message for ol in result] == ["a", "c"]


def test_read_tail_max_lines(backend, s3_client):
    lines = [OutputLine(f"l{i}", i) for i in range(1, 11)]
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl.gz", lines)

    result = backend.read_output(iid("job1", "run1"), max_lines=3)

    assert [ol.message for ol in result] == ["l8", "l9", "l10"]


def test_read_preserves_all_outputline_fields(backend, s3_client):
    line = OutputLine(
        message="msg",
        ordinal=1,
        is_error=True,
        source="PHASE_X",
        level="ERROR",
        logger="com.example",
        thread="worker-1",
        fields={"user_id": "u123", "count": 7},
    )
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl.gz", [line])

    [restored] = backend.read_output(iid("job1", "run1"))

    assert restored.message == "msg"
    assert restored.is_error is True
    assert restored.source == "PHASE_X"
    assert restored.level == "ERROR"
    assert restored.logger == "com.example"
    assert restored.thread == "worker-1"
    assert restored.fields == {"user_id": "u123", "count": 7}


def test_delete_output_removes_both_compressed_and_plain(backend, s3_client):
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl.gz", [OutputLine("x", 1)])
    _put_jsonl(s3_client, "prod/job1/run1__1.jsonl", [OutputLine("y", 1)], compressed=False)

    backend.delete_output(iid("job1", "run1"))

    keys = {o["Key"] for o in s3_client.list_objects_v2(Bucket=BUCKET).get("Contents", [])}
    assert "prod/job1/run1__1.jsonl.gz" not in keys
    assert "prod/job1/run1__1.jsonl" not in keys


def test_create_backend_factory_uses_endpoint_url():
    """parse_s3_config should accept endpoint_url for S3-compatible services."""
    cfg = OutputStorageConfig(type="s3", bucket=BUCKET, endpoint_url="http://localhost:9000")
    s3_cfg = parse_s3_config(cfg)
    assert s3_cfg.endpoint_url == "http://localhost:9000"
    assert s3_cfg.bucket == BUCKET


def test_create_backend_via_factory(s3_client):
    """create_backend builds a working S3OutputBackend from generic config."""
    cfg = OutputStorageConfig(type="s3", bucket=BUCKET, prefix="prod", region="us-east-1")
    backend = create_backend("env1", cfg)
    # Read against the moto-mocked bucket: we need to patch the backend's client.
    # Simpler: just verify it constructs the right type with the right bucket/prefix.
    assert backend.type == "s3"
    assert backend._bucket == BUCKET
    assert backend._prefix == "prod"


def test_backend_close_releases_client(backend):
    """backend.close() should call the client's close() if available — that's how
    we release the underlying connection pool. Writers must NOT do this; only the
    owner (backend/store) does.
    """
    from unittest.mock import MagicMock
    backend._client = MagicMock()
    backend.close()
    backend._client.close.assert_called_once()


def test_backend_close_tolerates_missing_client_close(backend):
    """Older botocore versions don't expose client.close(); the backend must
    handle that gracefully and not crash on close().
    """
    class _NoClose:
        pass
    backend._client = _NoClose()
    backend.close()  # must not raise
