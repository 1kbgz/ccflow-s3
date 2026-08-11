import os
from datetime import UTC, datetime
from json import dumps
from pathlib import Path
from time import sleep
from typing import Any, Literal
from urllib.parse import urlparse

from boto3 import Session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError, ResponseStreamingError
from ccflow import (
    BaseModel,
    CallableModel,
    Flow,
    GenericResult,
    NullContext,
)
from ccflow_etl import APIKeySecretCredentials, CacheFormat, PayloadCodec
from pydantic import Field

__all__ = (
    "S3ArtifactStore",
    "S3Auth",
    "S3CacheStore",
    "S3Client",
    "S3ClientCredentials",
    "S3Config",
    "S3Context",
    "S3CopyContext",
    "S3CopyResult",
    "S3Credentials",
    "S3DeleteContext",
    "S3DeleteResult",
    "S3ExistsContext",
    "S3ExistsResult",
    "S3HeadContext",
    "S3HeadResult",
    "S3ListContext",
    "S3ListResult",
    "S3Model",
    "S3ObjectManifest",
    "S3PrefixWalkContext",
    "S3Provider",
    "S3ReadContext",
    "S3ReadResult",
    "S3ReadWriteContext",
    "S3ReadWriteResult",
    "S3Result",
    "S3Session",
    "S3WriteDataContext",
    "S3WriteResult",
)


def _env_value(name: str | None) -> str | None:
    return os.environ.get(name) if name else None


def _first_configured(*values: str | None) -> str | None:
    return next((value for value in values if value not in (None, "")), None)


_STREAM_RETRY_EXCEPTIONS = (ResponseStreamingError, ConnectionClosedError, ReadTimeoutError)


def _get_object_bytes(client: Any, *, max_attempts: int = 5, wait_initial: float = 1.0, **kwargs: Any) -> bytes:
    for attempt in range(1, max_attempts + 1):
        try:
            return _read_object_body(client.get_object(**kwargs))
        except _STREAM_RETRY_EXCEPTIONS:
            if attempt >= max_attempts:
                raise
            sleep(wait_initial * 2 ** (attempt - 1))
    raise RuntimeError("unreachable")


def _read_object_body(response: dict[str, Any]) -> bytes:
    body = response["Body"]
    try:
        return body.read()
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()


class S3Config(BaseModel):
    signature_version: str | None = "s3v4"
    addressing_style: Literal["auto", "virtual", "path"] | None = None
    connect_timeout: float | None = None
    read_timeout: float | None = None
    retries: dict[str, Any] = Field(default_factory=dict)
    user_agent_extra: str | None = None

    def create_config(self, *, unsigned: bool = False) -> Config:
        kwargs: dict[str, Any] = {}
        if unsigned:
            kwargs["signature_version"] = UNSIGNED
        elif self.signature_version:
            kwargs["signature_version"] = self.signature_version
        if self.addressing_style:
            kwargs["s3"] = {"addressing_style": self.addressing_style}
        if self.connect_timeout is not None:
            kwargs["connect_timeout"] = self.connect_timeout
        if self.read_timeout is not None:
            kwargs["read_timeout"] = self.read_timeout
        if self.retries:
            kwargs["retries"] = self.retries
        if self.user_agent_extra:
            kwargs["user_agent_extra"] = self.user_agent_extra
        return Config(**kwargs)

    @property
    def config(self) -> Config:
        return self.create_config()


class S3Credentials(APIKeySecretCredentials):
    api_key_env: str | None = "AWS_ACCESS_KEY_ID"
    secret_key_env: str | None = "AWS_SECRET_ACCESS_KEY"


class S3ClientCredentials(BaseModel):
    mode: Literal["default", "profile", "access_key", "env", "credentials", "anonymous"] = "default"
    credentials: APIKeySecretCredentials | None = None
    access_key_id: str | None = Field(default=None, repr=False)
    secret_access_key: str | None = Field(default=None, repr=False)
    session_token: str | None = Field(default=None, repr=False)
    access_key_id_env: str | None = None
    secret_access_key_env: str | None = None
    session_token_env: str | None = None
    profile_name: str | None = None
    region_name: str | None = None
    region_name_env: str | None = None

    @property
    def is_anonymous(self) -> bool:
        return self.mode == "anonymous"

    def resolved_region_name(self) -> str | None:
        return _first_configured(self.region_name, _env_value(self.region_name_env))

    def _resolved_access_key_id(self) -> str | None:
        if self.credentials:
            return _first_configured(self.access_key_id, self.credentials.resolved_api_key(), _env_value(self.access_key_id_env))
        return _first_configured(self.access_key_id, _env_value(self.access_key_id_env))

    def _resolved_secret_access_key(self) -> str | None:
        if self.credentials:
            return _first_configured(self.secret_access_key, self.credentials.resolved_secret_key(), _env_value(self.secret_access_key_env))
        return _first_configured(self.secret_access_key, _env_value(self.secret_access_key_env))

    def _resolved_session_token(self) -> str | None:
        return _first_configured(self.session_token, _env_value(self.session_token_env))

    def session_kwargs(self, region_name: str | None = None) -> dict[str, str]:
        if self.mode == "anonymous":
            return {}
        if self.mode == "default":
            return {key: value for key, value in {"region_name": region_name or self.resolved_region_name()}.items() if value is not None}
        if self.mode == "profile":
            kwargs = {"profile_name": self.profile_name, "region_name": region_name or self.resolved_region_name()}
            return {key: value for key, value in kwargs.items() if value is not None}

        kwargs = {
            "aws_access_key_id": self._resolved_access_key_id(),
            "aws_secret_access_key": self._resolved_secret_access_key(),
            "aws_session_token": self._resolved_session_token(),
            "region_name": region_name or self.resolved_region_name(),
        }
        return {key: value for key, value in kwargs.items() if value is not None}

    def create_session(self, region_name: str | None = None) -> Session:
        return Session(**self.session_kwargs(region_name=region_name))


S3Auth = S3ClientCredentials


class S3Provider(BaseModel):
    name: Literal["aws", "backblaze", "hetzner", "cloudflare", "custom"] = "aws"
    endpoint_url: str | None = None
    endpoint_url_env: str | None = None
    region_name: str | None = None
    region_name_env: str | None = None
    account_id: str | None = None
    account_id_env: str | None = None

    def resolved_region_name(self) -> str | None:
        region = _first_configured(self.region_name, _env_value(self.region_name_env))
        if self.name == "aws":
            return region or _env_value("AWS_DEFAULT_REGION")
        if self.name == "cloudflare":
            return region or "auto"
        return region

    def resolved_endpoint_url(self) -> str | None:
        endpoint_url = _first_configured(self.endpoint_url, _env_value(self.endpoint_url_env))
        if endpoint_url:
            return endpoint_url
        region = self.resolved_region_name()
        if self.name == "aws":
            return None
        if self.name == "backblaze" and region:
            return f"https://s3.{region}.backblazeb2.com"
        if self.name == "hetzner" and region:
            return f"https://{region}.your-objectstorage.com"
        if self.name == "cloudflare":
            account_id = _first_configured(self.account_id, _env_value(self.account_id_env))
            if account_id:
                return f"https://{account_id}.r2.cloudflarestorage.com"
        if self.name == "custom":
            raise ValueError("S3Provider custom requires endpoint_url or endpoint_url_env.")
        raise ValueError(f"S3Provider {self.name} requires endpoint_url, endpoint_url_env, or provider-specific endpoint fields.")


class S3Session(BaseModel):
    credentials: APIKeySecretCredentials | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    profile_name: str | None = None
    region_name: str | None = None

    def _session_kwargs(self, region_name: str | None = None) -> dict[str, str]:
        credentials = self.credentials
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id or (credentials.resolved_api_key() if credentials else None),
            "aws_secret_access_key": self.aws_secret_access_key or (credentials.resolved_secret_key() if credentials else None),
            "aws_session_token": self.aws_session_token,
            "profile_name": self.profile_name,
            "region_name": self.region_name or region_name,
        }
        return {key: value for key, value in kwargs.items() if value is not None}

    def create_session(self, region_name: str | None = None) -> Session:
        return Session(**self._session_kwargs(region_name=region_name))

    @property
    def session(self) -> Session:
        return self.create_session()


class S3Client(BaseModel):
    endpoint_url: str | None = None
    provider: S3Provider | None = None
    credentials: S3ClientCredentials | None = None
    auth: S3ClientCredentials | None = None
    session: S3Session = Field(default_factory=S3Session)
    config: S3Config = Field(default_factory=S3Config)
    region_name: str | None = None

    def _credentials(self) -> S3ClientCredentials | None:
        return self.credentials or self.auth

    def resolved_region_name(self) -> str | None:
        provider_region = self.provider.resolved_region_name() if self.provider else None
        credentials = self._credentials()
        auth_region = credentials.resolved_region_name() if credentials else None
        return _first_configured(self.region_name, provider_region, auth_region, self.session.region_name)

    def resolved_endpoint_url(self) -> str | None:
        endpoint_url = _first_configured(self.endpoint_url)
        if endpoint_url:
            return endpoint_url
        if self.provider:
            return self.provider.resolved_endpoint_url()
        return None

    def create_session(self, region_name: str | None = None) -> Session:
        credentials = self._credentials()
        if credentials:
            return credentials.create_session(region_name=region_name)
        return self.session.create_session(region_name=region_name)

    def create_config(self) -> Config:
        credentials = self._credentials()
        return self.config.create_config(unsigned=bool(credentials and credentials.is_anonymous))

    @property
    def client(self):
        region_name = self.resolved_region_name()
        return self.create_session(region_name=region_name).client(
            "s3",
            endpoint_url=self.resolved_endpoint_url(),
            region_name=region_name,
            config=self.create_config(),
        )


class S3Context(NullContext):
    bucket: str | None = None
    object: str | None = None


class S3ReadContext(S3Context): ...


class S3ExistsContext(S3Context): ...


class S3HeadContext(S3Context): ...


class S3ListContext(S3Context):
    prefix: str = ""
    page_size: int | None = None


class S3PrefixWalkContext(S3ListContext): ...


class S3CopyContext(S3Context):
    source_bucket: str
    source_object: str
    content_type: str | None = None


class S3DeleteContext(S3Context): ...


class S3WriteContext(S3Context):
    overwrite: bool = False
    content_type: str | None = None
    atomic: bool = False
    temp_object: str | None = None
    manifest_object: str | None = None
    row_count: int | None = None
    producer: dict[str, Any] = Field(default_factory=dict)


class S3WriteDataContext(S3WriteContext):
    data: bytes | str | dict | list[dict[str, Any]]


class S3ReadWriteContext(S3Context):
    read: S3ReadContext
    write: S3WriteDataContext


class S3Result(GenericResult): ...


class S3ReadResult(S3Result): ...


class S3ExistsResult(S3Result): ...


class S3HeadResult(S3Result): ...


class S3ListResult(S3Result): ...


class S3CopyResult(S3Result): ...


class S3DeleteResult(S3Result): ...


class S3WriteResult(S3Result): ...


class S3ReadWriteResult(S3Result):
    read: S3ReadResult
    write: S3WriteResult


class S3ObjectManifest(BaseModel):
    bucket: str
    object: str
    size: int
    etag: str | None = None
    content_type: str | None = None
    row_count: int | None = None
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    producer: dict[str, Any] = Field(default_factory=dict)


class S3CacheStore(BaseModel):
    client: S3Client
    bucket: str
    prefix: str = ""

    def object_key(self, key: str) -> str:
        clean_prefix = self.prefix.strip("/")
        clean_key = key.lstrip("/")
        return f"{clean_prefix}/{clean_key}" if clean_prefix else clean_key

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(key)}"

    def _object_key(self, key: str) -> str:
        return self.object_key(key)

    def exists(self, key: str) -> bool:
        try:
            self.client.client.head_object(Bucket=self.bucket, Key=self._object_key(key))
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def get_bytes(self, key: str) -> bytes:
        return _get_object_bytes(self.client.client, Bucket=self.bucket, Key=self._object_key(key))

    def put_bytes(self, key: str, value: bytes, content_type: str | None = None) -> dict[str, Any]:
        kwargs = {"Bucket": self.bucket, "Key": self._object_key(key), "Body": value}
        if content_type:
            kwargs["ContentType"] = content_type
        response = self.client.client.put_object(**kwargs)
        return {"bucket": self.bucket, "object": self._object_key(key), "etag": response.get("ETag")}


class S3ArtifactStore(BaseModel):
    client: S3Client
    bucket: str
    prefix: str = ""

    def object_key(self, key: str) -> str:
        clean_prefix = self.prefix.strip("/")
        clean_key = key.lstrip("/")
        return f"{clean_prefix}/{clean_key}" if clean_prefix else clean_key

    def artifact_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{self.object_key(key)}"

    def exists(self, key: str) -> bool:
        try:
            self.client.client.head_object(Bucket=self.bucket, Key=self.object_key(key))
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> list[str]:
        object_prefix = self.object_key(prefix)
        configured_prefix = self.prefix.strip("/")
        if not prefix and configured_prefix:
            object_prefix = f"{configured_prefix}/"
        keys = []
        continuation_token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": object_prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []):
                object_key = str(item["Key"])
                if configured_prefix:
                    object_key = object_key.removeprefix(f"{configured_prefix}/")
                keys.append(object_key)
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break
        return sorted(keys)

    def read(self, key: str) -> bytes:
        return _get_object_bytes(self.client.client, Bucket=self.bucket, Key=self.object_key(key))

    def read_file(self, key: str, path: str | Path) -> dict[str, Any]:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.client.download_file(Bucket=self.bucket, Key=self.object_key(key), Filename=str(output_path))
        return {
            "bucket": self.bucket,
            "object": self.object_key(key),
            "path": str(output_path),
            "size": output_path.stat().st_size,
            "status": "materialized",
        }

    def get_bytes(self, key: str) -> bytes:
        return self.read(key)

    def write(self, key: str, payload: bytes, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        kwargs = {"Bucket": self.bucket, "Key": self.object_key(key), "Body": payload}
        if media_type:
            kwargs["ContentType"] = media_type
        if metadata:
            kwargs["Metadata"] = {str(metadata_key): str(metadata_value) for metadata_key, metadata_value in metadata.items()}
        response = self.client.client.put_object(**kwargs)
        return {"bucket": self.bucket, "object": self.object_key(key), "etag": response.get("ETag"), "status": "written"}

    def write_file(self, key: str, path: str | Path, media_type: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        source_path = Path(path)
        kwargs = {"Bucket": self.bucket, "Key": self.object_key(key)}
        if media_type:
            kwargs["ContentType"] = media_type
        if metadata:
            kwargs["Metadata"] = {str(metadata_key): str(metadata_value) for metadata_key, metadata_value in metadata.items()}
        with source_path.open("rb") as file_obj:
            response = self.client.client.put_object(Body=file_obj, **kwargs)
        return {
            "bucket": self.bucket,
            "object": self.object_key(key),
            "etag": response.get("ETag"),
            "path": str(source_path),
            "size": source_path.stat().st_size,
            "status": "written",
        }

    def _source(self, source_key: str | None, source_uri: str | None) -> dict[str, str]:
        if source_key:
            return {"Bucket": self.bucket, "Key": self.object_key(source_key)}
        if source_uri:
            parsed = urlparse(source_uri)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
                raise ValueError("S3ArtifactStore source_uri must be an s3://bucket/key URI.")
            return {"Bucket": parsed.netloc, "Key": parsed.path.lstrip("/")}
        raise ValueError("S3ArtifactStore.publish requires source_key or source_uri.")

    def publish(
        self, key: str, source_key: str | None = None, source_uri: str | None = None, metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        response = self.client.client.copy_object(Bucket=self.bucket, Key=self.object_key(key), CopySource=self._source(source_key, source_uri))
        copy_result = response.get("CopyObjectResult", {})
        return {
            "bucket": self.bucket,
            "object": self.object_key(key),
            "source_key": source_key,
            "source_uri": source_uri,
            "etag": copy_result.get("ETag"),
            "status": "published",
            **(metadata or {}),
        }


class S3Model(CallableModel):
    bucket: str | None = None
    object: str | None = None
    client: S3Client

    mode: Literal["read", "write", "read_write", "exists", "head", "list", "prefix_walk", "copy", "delete"] = "read"
    format: CacheFormat = "binary"

    @property
    def codec(self) -> PayloadCodec:
        return PayloadCodec(format=self.format)

    def _read_data(self, client: S3Client, bucket: str, object: str) -> S3ReadResult:
        return S3ReadResult(value=self.codec.decode(_get_object_bytes(client.client, Bucket=bucket, Key=object)))

    def _object_exists(self, client: S3Client, bucket: str, object: str) -> bool:
        try:
            client.client.head_object(Bucket=bucket, Key=object)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def _head_object(self, client: S3Client, bucket: str, object: str) -> S3HeadResult:
        response = client.client.head_object(Bucket=bucket, Key=object)
        return S3HeadResult(
            value={
                "bucket": bucket,
                "object": object,
                "etag": response.get("ETag"),
                "content_length": response.get("ContentLength"),
                "content_type": response.get("ContentType"),
                "metadata": response.get("Metadata", {}),
            }
        )

    def _list_objects(self, client: S3Client, bucket: str, prefix: str, page_size: int | None = None) -> S3ListResult:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if page_size:
            kwargs["MaxKeys"] = page_size
        response = client.client.list_objects_v2(**kwargs)
        objects = [{"key": item.get("Key"), "size": item.get("Size"), "etag": item.get("ETag")} for item in response.get("Contents", [])]
        return S3ListResult(value={"bucket": bucket, "prefix": prefix, "objects": objects})

    def _walk_objects(self, client: S3Client, bucket: str, prefix: str, page_size: int | None = None) -> S3ListResult:
        objects = []
        continuation_token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            if page_size:
                kwargs["MaxKeys"] = page_size
            response = client.client.list_objects_v2(**kwargs)
            objects.extend({"key": item.get("Key"), "size": item.get("Size"), "etag": item.get("ETag")} for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                break
        return S3ListResult(value={"bucket": bucket, "prefix": prefix, "objects": objects})

    def _copy_object(self, client: S3Client, bucket: str, object: str, context: S3CopyContext) -> S3CopyResult:
        kwargs = {
            "Bucket": bucket,
            "Key": object,
            "CopySource": {"Bucket": context.source_bucket, "Key": context.source_object},
        }
        if context.content_type:
            kwargs["ContentType"] = context.content_type
        response = client.client.copy_object(**kwargs)
        copy_result = response.get("CopyObjectResult", {})
        return S3CopyResult(
            value={
                "bucket": bucket,
                "object": object,
                "source_bucket": context.source_bucket,
                "source_object": context.source_object,
                "status": "copied",
                "etag": copy_result.get("ETag"),
            }
        )

    def _delete_object(self, client: S3Client, bucket: str, object: str) -> S3DeleteResult:
        response = client.client.delete_object(Bucket=bucket, Key=object)
        return S3DeleteResult(value={"bucket": bucket, "object": object, "deleted": True, "delete_marker": response.get("DeleteMarker")})

    def _write_body(self, data: bytes | str | dict | list[dict[str, Any]]) -> bytes:
        return self.codec.encode(data)

    def _content_type(self, context: S3WriteContext) -> str | None:
        if context.content_type:
            return context.content_type
        return self.codec.media_type

    def _manifest_payload(
        self, bucket: str, object: str, body: bytes, content_type: str | None, etag: str | None, context: S3WriteContext
    ) -> S3ObjectManifest:
        return S3ObjectManifest(
            bucket=bucket,
            object=object,
            size=len(body),
            etag=etag,
            content_type=content_type,
            row_count=context.row_count,
            producer=context.producer,
        )

    def _write_manifest(self, client: S3Client, bucket: str, manifest_object: str, manifest: S3ObjectManifest) -> dict[str, Any]:
        body = dumps(manifest.model_dump(), separators=(",", ":")).encode("utf-8")
        response = client.client.put_object(Bucket=bucket, Key=manifest_object, Body=body, ContentType="application/json")
        return {"bucket": bucket, "object": manifest_object, "etag": response.get("ETag")}

    def _write_data(self, client: S3Client, bucket: str, object: str, context: S3WriteDataContext) -> S3WriteResult:
        if self._object_exists(client, bucket, object) and not context.overwrite:
            return S3WriteResult(value={"bucket": bucket, "object": object, "status": "exists", "written": False})

        body = self._write_body(context.data)
        content_type = self._content_type(context)
        target_object = context.temp_object or f"{object}.tmp" if context.atomic else object
        kwargs = {"Bucket": bucket, "Key": target_object, "Body": body}
        if content_type:
            kwargs["ContentType"] = content_type
        response = client.client.put_object(**kwargs)
        etag = response.get("ETag")
        if context.atomic:
            copy_result = self._copy_object(
                client,
                bucket,
                object,
                S3CopyContext(bucket=bucket, object=object, source_bucket=bucket, source_object=target_object, content_type=content_type),
            )
            etag = copy_result.value.get("etag")

        manifest = None
        if context.manifest_object:
            manifest_payload = self._manifest_payload(bucket=bucket, object=object, body=body, content_type=content_type, etag=etag, context=context)
            manifest = self._write_manifest(client, bucket, context.manifest_object, manifest_payload)

        return S3WriteResult(
            value={
                "bucket": bucket,
                "object": object,
                "status": "written",
                "written": True,
                "etag": etag,
                "temp_object": target_object if context.atomic else None,
                "manifest": manifest,
            }
        )

    @property
    def context_type(self):
        return S3Context

    @property
    def result_type(self):
        return S3Result

    @Flow.call
    def __call__(self, context: S3Context) -> S3Result:
        if isinstance(context, S3ReadWriteContext):
            read_bucket = context.read.bucket or self.bucket
            read_object = context.read.object or self.object
            write_bucket = context.write.bucket or self.bucket
            write_object = context.write.object or self.object
            if not read_bucket or not read_object or not write_bucket or not write_object:
                raise ValueError("read_write mode requires read and write bucket/object values.")
            read_result = self._read_data(self.client, read_bucket, read_object)
            write_result = self._write_data(self.client, write_bucket, write_object, context.write)
            return S3ReadWriteResult(value={"read": read_result.value, "write": write_result.value}, read=read_result, write=write_result)

        bucket = context.bucket or self.bucket
        object = context.object or self.object

        if not bucket:
            raise ValueError("A bucket must be specified either in the model or the context.")

        if isinstance(context, S3PrefixWalkContext) or self.mode == "prefix_walk":
            if not isinstance(context, S3PrefixWalkContext):
                raise ValueError("prefix_walk mode requires S3PrefixWalkContext")
            return self._walk_objects(self.client, bucket, context.prefix, context.page_size)

        if isinstance(context, S3ListContext) or self.mode == "list":
            prefix = context.prefix if isinstance(context, S3ListContext) else object or ""
            page_size = context.page_size if isinstance(context, S3ListContext) else None
            return self._list_objects(self.client, bucket, prefix, page_size)

        if not object:
            raise ValueError("An object must be specified either in the model or the context.")

        if isinstance(context, S3ExistsContext) or self.mode == "exists":
            return S3ExistsResult(value=self._object_exists(self.client, bucket, object))

        if isinstance(context, S3HeadContext) or self.mode == "head":
            return self._head_object(self.client, bucket, object)

        if isinstance(context, S3CopyContext) or self.mode == "copy":
            if not isinstance(context, S3CopyContext):
                raise ValueError("copy mode requires S3CopyContext")
            return self._copy_object(self.client, bucket, object, context)

        if isinstance(context, S3DeleteContext) or self.mode == "delete":
            if not isinstance(context, S3DeleteContext):
                raise ValueError("delete mode requires S3DeleteContext")
            return self._delete_object(self.client, bucket, object)

        if isinstance(context, S3ReadContext) or self.mode in ["read", "read_write"]:
            read_result = self._read_data(self.client, bucket, object)
        else:
            read_result = None

        if isinstance(context, S3WriteDataContext) and self.mode in ["write", "read_write"]:
            write_result = self._write_data(self.client, bucket, object, context)
        else:
            write_result = None

        if read_result and write_result:
            return S3ReadWriteResult(value={"read": read_result.value, "write": write_result.value}, read=read_result, write=write_result)
        elif read_result:
            return read_result
        elif write_result:
            return write_result
        else:
            raise ValueError("No operation performed; check mode and context types.")
