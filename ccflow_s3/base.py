from datetime import datetime, timezone
from json import dumps
from typing import Any, Dict, List, Literal, Optional, Union
from urllib.parse import quote

from boto3 import Session
from botocore.config import Config
from botocore.exceptions import ClientError
from ccflow import (
    BaseModel,
    CallableModel,
    Flow,
    GenericResult,
    NullContext,
)
from ccflow_etl import APIKeySecretCredentials, CacheFormat, CheckpointRecord, CheckpointStatus, PayloadCodec

try:
    from orjson import loads
except ImportError:
    from json import loads
from pydantic import Field

__all__ = (
    "S3Config",
    "S3Credentials",
    "S3Session",
    "S3Client",
    "S3Context",
    "S3ExistsContext",
    "S3HeadContext",
    "S3ListContext",
    "S3CopyContext",
    "S3DeleteContext",
    "S3PrefixWalkContext",
    "S3ReadContext",
    "S3ReadWriteContext",
    "S3WriteDataContext",
    "S3CacheStore",
    "S3CheckpointStore",
    "S3ObjectManifest",
    "S3Result",
    "S3ExistsResult",
    "S3HeadResult",
    "S3ListResult",
    "S3CopyResult",
    "S3DeleteResult",
    "S3ReadResult",
    "S3ReadWriteResult",
    "S3WriteResult",
    "S3Model",
)


class S3Config(BaseModel):
    signature_version: str = "s3v4"

    @property
    def config(self) -> Config:
        return Config(signature_version=self.signature_version)


class S3Credentials(APIKeySecretCredentials):
    api_key_env: Optional[str] = "AWS_ACCESS_KEY_ID"
    secret_key_env: Optional[str] = "AWS_SECRET_ACCESS_KEY"


class S3Session(BaseModel):
    credentials: Optional[APIKeySecretCredentials] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    region_name: Optional[str] = None

    def _session_kwargs(self, region_name: Optional[str] = None) -> Dict[str, str]:
        credentials = self.credentials
        kwargs = {
            "aws_access_key_id": self.aws_access_key_id or (credentials.resolved_api_key() if credentials else None),
            "aws_secret_access_key": self.aws_secret_access_key or (credentials.resolved_secret_key() if credentials else None),
            "aws_session_token": self.aws_session_token,
            "profile_name": self.profile_name,
            "region_name": self.region_name or region_name,
        }
        return {key: value for key, value in kwargs.items() if value is not None}

    def create_session(self, region_name: Optional[str] = None) -> Session:
        return Session(**self._session_kwargs(region_name=region_name))

    @property
    def session(self) -> Session:
        return self.create_session()


class S3Client(BaseModel):
    endpoint_url: Optional[str] = None
    session: S3Session = Field(default_factory=S3Session)
    config: S3Config = Field(default_factory=S3Config)
    region_name: Optional[str] = None

    @property
    def client(self):
        return self.session.create_session(region_name=self.region_name).client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=self.config.config,
        )


class S3Context(NullContext):
    bucket: Optional[str] = None
    object: Optional[str] = None


class S3ReadContext(S3Context): ...


class S3ExistsContext(S3Context): ...


class S3HeadContext(S3Context): ...


class S3ListContext(S3Context):
    prefix: str = ""
    page_size: Optional[int] = None


class S3PrefixWalkContext(S3ListContext): ...


class S3CopyContext(S3Context):
    source_bucket: str
    source_object: str
    content_type: Optional[str] = None


class S3DeleteContext(S3Context): ...


class S3WriteContext(S3Context):
    overwrite: bool = False
    content_type: Optional[str] = None
    atomic: bool = False
    temp_object: Optional[str] = None
    manifest_object: Optional[str] = None
    row_count: Optional[int] = None
    producer: Dict[str, Any] = Field(default_factory=dict)


class S3WriteDataContext(S3WriteContext):
    data: Union[bytes, str, dict, List[Dict[str, Any]]]


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
    etag: Optional[str] = None
    content_type: Optional[str] = None
    row_count: Optional[int] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    producer: Dict[str, Any] = Field(default_factory=dict)


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
        return self.client.client.get_object(Bucket=self.bucket, Key=self._object_key(key))["Body"].read()

    def put_bytes(self, key: str, value: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
        kwargs = {"Bucket": self.bucket, "Key": self._object_key(key), "Body": value}
        if content_type:
            kwargs["ContentType"] = content_type
        response = self.client.client.put_object(**kwargs)
        return {"bucket": self.bucket, "object": self._object_key(key), "etag": response.get("ETag")}


class S3CheckpointStore(BaseModel):
    client: S3Client
    bucket: str
    prefix: str = "checkpoints"

    def _object_key(self, key: str) -> str:
        clean_prefix = self.prefix.strip("/")
        encoded_key = quote(key, safe="")
        return f"{clean_prefix}/{encoded_key}.json" if clean_prefix else f"{encoded_key}.json"

    def get(self, key: str) -> Optional[CheckpointRecord]:
        try:
            body = self.client.client.get_object(Bucket=self.bucket, Key=self._object_key(key))["Body"].read()
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return CheckpointRecord(**loads(body))

    def mark(self, key: str, status: CheckpointStatus, metadata: Optional[Dict[str, Any]] = None) -> CheckpointRecord:
        record = CheckpointRecord(
            key=key,
            status=status,
            updated_at=datetime.now(timezone.utc).isoformat(),
            metadata=metadata or {},
        )
        self.client.client.put_object(
            Bucket=self.bucket,
            Key=self._object_key(key),
            Body=dumps(record.model_dump(), separators=(",", ":"), sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return record

    def mark_succeeded(self, key: str, metadata: Optional[Dict[str, Any]] = None) -> CheckpointRecord:
        return self.mark(key=key, status="succeeded", metadata=metadata)

    def should_skip(self, key: str) -> bool:
        record = self.get(key)
        return record is not None and record.status == "succeeded"


class S3Model(CallableModel):
    bucket: Optional[str] = None
    object: Optional[str] = None
    client: S3Client

    mode: Literal["read", "write", "read_write", "exists", "head", "list", "prefix_walk", "copy", "delete"] = "read"
    format: CacheFormat = "binary"

    @property
    def codec(self) -> PayloadCodec:
        return PayloadCodec(format=self.format)

    def _read_data(self, client: S3Client, bucket: str, object: str) -> S3ReadResult:
        read_response = client.client.get_object(Bucket=bucket, Key=object)

        return S3ReadResult(value=self.codec.decode(read_response["Body"].read()))

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

    def _list_objects(self, client: S3Client, bucket: str, prefix: str, page_size: Optional[int] = None) -> S3ListResult:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if page_size:
            kwargs["MaxKeys"] = page_size
        response = client.client.list_objects_v2(**kwargs)
        objects = [{"key": item.get("Key"), "size": item.get("Size"), "etag": item.get("ETag")} for item in response.get("Contents", [])]
        return S3ListResult(value={"bucket": bucket, "prefix": prefix, "objects": objects})

    def _walk_objects(self, client: S3Client, bucket: str, prefix: str, page_size: Optional[int] = None) -> S3ListResult:
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

    def _write_body(self, data: Union[bytes, str, dict, List[Dict[str, Any]]]) -> bytes:
        return self.codec.encode(data)

    def _content_type(self, context: S3WriteContext) -> Optional[str]:
        if context.content_type:
            return context.content_type
        return self.codec.media_type

    def _manifest_payload(
        self, bucket: str, object: str, body: bytes, content_type: Optional[str], etag: Optional[str], context: S3WriteContext
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

    def _write_manifest(self, client: S3Client, bucket: str, manifest_object: str, manifest: S3ObjectManifest) -> Dict[str, Any]:
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
