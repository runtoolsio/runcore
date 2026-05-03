"""S3-backed output backend — JSONL storage in object storage.

Read-side module contract:
    create_backend(env_id, config) -> S3OutputBackend

Object layout:
    s3://{bucket}/{prefix}/{job_id}/{run_id}__{ordinal}.jsonl[.gz]

Designed for PaaS deployments (fly.io, Cloud Run, ECS Fargate) where local disk is
ephemeral. The writer (runjob/output/s3.py) buffers lines in memory and PUTs once on
close. Compatible with any S3-API service via ``endpoint_url`` (R2, MinIO, LocalStack).
"""

import gzip
import logging
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from runtools.runcore.output import (
    OutputBackend,
    OutputLine,
    OutputReadError,
    OutputStorageConfig,
    parse_jsonl_bytes,
)

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as e:
    raise ImportError(
        "S3 output backend requires boto3. Install with: "
        "pip install runtoolsio-runcore[s3]"
    ) from e

log = logging.getLogger(__name__)


class S3StorageConfig(BaseModel):
    """S3 backend config — validated internally from the generic OutputStorageConfig extras."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix under the bucket")
    region: Optional[str] = Field(default=None, description="AWS region; default chain if None")
    endpoint_url: Optional[str] = Field(default=None,
                                        description="S3-compatible endpoint (R2, MinIO, LocalStack)")
    profile: Optional[str] = Field(default=None, description="boto3 named profile")
    compress: bool = Field(default=True, description="Gzip-compress objects (.jsonl.gz)")


def parse_s3_config(config: OutputStorageConfig) -> S3StorageConfig:
    """Validate and extract S3-specific config from generic storage config extras."""
    return S3StorageConfig.model_validate(dict(config.model_extra or {}))


def build_client(cfg: S3StorageConfig):
    """Build a boto3 S3 client from validated config. Uses the default credential chain."""
    session = boto3.Session(profile_name=cfg.profile) if cfg.profile else boto3.Session()
    kwargs = {}
    if cfg.region:
        kwargs["region_name"] = cfg.region
    if cfg.endpoint_url:
        kwargs["endpoint_url"] = cfg.endpoint_url
    return session.client("s3", **kwargs)


def object_key(prefix: str, instance_id, *, compress: bool) -> str:
    """Build the S3 key for an instance's output object."""
    suffix = ".jsonl.gz" if compress else ".jsonl"
    base = f"{instance_id.job_id}/{instance_id.run_id}__{instance_id.ordinal}{suffix}"
    if not prefix:
        return base
    return f"{prefix.strip('/')}/{base}"


def _is_not_found(err: 'ClientError') -> bool:
    code = err.response.get("Error", {}).get("Code", "")
    return code in ("NoSuchKey", "404", "NotFound")


class S3OutputBackend(OutputBackend):
    """S3-backed output backend. Reads JSONL objects (plain or gzipped)."""

    type = "s3"

    def __init__(self, client, bucket: str, prefix: str = ""):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _resolve_key(self, instance_id) -> tuple[str, bool] | None:
        """Find the actual object key. Returns (key, is_compressed) or None if neither exists.

        Raises:
            ClientError: For any non-404 S3 error (permission denied, network, etc.).
                read_output wraps these into OutputReadError.
        """
        for compressed in (True, False):
            key = object_key(self._prefix, instance_id, compress=compressed)
            try:
                self._client.head_object(Bucket=self._bucket, Key=key)
                return key, compressed
            except ClientError as ex:
                if _is_not_found(ex):
                    continue
                raise
        return None

    def read_output(self, instance_id, sources: set[str] | None = None,
                    max_lines: int = 0) -> List[OutputLine]:
        location = f"s3://{self._bucket}/{object_key(self._prefix, instance_id, compress=True)}"
        try:
            resolved = self._resolve_key(instance_id)
            if resolved is None:
                return []
            key, compressed = resolved
            location = f"s3://{self._bucket}/{key}"
            obj = self._client.get_object(Bucket=self._bucket, Key=key)
            data = obj["Body"].read()
        except ClientError as ex:
            if _is_not_found(ex):
                return []
            raise OutputReadError(location, ex) from ex
        except BotoCoreError as ex:
            # Transport-layer failures (DNS, connection, timeout) — no error code
            # to inspect, so always wrap. MultiSourceOutputReader can then try
            # the next backend.
            raise OutputReadError(location, ex) from ex

        if compressed:
            try:
                data = gzip.decompress(data)
            except (OSError, EOFError) as ex:
                raise OutputReadError(location, ex) from ex

        try:
            lines = parse_jsonl_bytes(data, sources)
        except (ValueError, KeyError) as ex:
            raise OutputReadError(location, ex) from ex

        if max_lines > 0:
            lines = lines[-max_lines:]
        return lines

    def delete_output(self, *instance_ids) -> None:
        for iid in instance_ids:
            for compressed in (True, False):
                key = object_key(self._prefix, iid, compress=compressed)
                try:
                    self._client.delete_object(Bucket=self._bucket, Key=key)
                except ClientError as ex:
                    if _is_not_found(ex):
                        continue
                    log.warning("Failed to delete S3 object",
                                extra={"bucket": self._bucket, "key": key}, exc_info=True)
                except BotoCoreError:
                    log.warning("Failed to delete S3 object (transport error)",
                                extra={"bucket": self._bucket, "key": key}, exc_info=True)

    def close(self):
        """Release the boto3 client's connection pool.

        The backend/store owns the client created in ``create_backend`` /
        ``create_store``; writers borrow it. Writers must not close the client
        because retention/read paths and later writers reuse it. Botocore added
        ``client.close()`` in modern versions; older builds without it skip.
        """
        client_close = getattr(self._client, "close", None)
        if client_close:
            client_close()


def create_backend(env_id: str, config: OutputStorageConfig) -> S3OutputBackend:
    """Module-level factory (read side) — part of the output backend module contract."""
    del env_id  # bucket+prefix are explicit; env_id not used for S3
    s3_cfg = parse_s3_config(config)
    client = build_client(s3_cfg)
    return S3OutputBackend(client, s3_cfg.bucket, s3_cfg.prefix)
