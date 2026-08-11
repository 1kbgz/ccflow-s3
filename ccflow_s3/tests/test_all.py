import json
import tomllib
from gzip import decompress
from pathlib import Path

import pytest
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from ccflow_etl import (
    APIKeySecretCredentials,
    ArtifactWriteModel,
    CacheGetContext,
    CacheGetModel,
    CachePutContext,
    CachePutModel,
)
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

import ccflow_s3.base as s3_base
from ccflow_s3 import (
    S3ArtifactStore,
    S3CacheStore,
    S3Client,
    S3ClientCredentials,
    S3CopyContext,
    S3Credentials,
    S3DeleteContext,
    S3ExistsContext,
    S3HeadContext,
    S3ListContext,
    S3Model,
    S3PrefixWalkContext,
    S3Provider,
    S3ReadContext,
    S3ReadWriteContext,
    S3Session,
    S3WriteDataContext,
)


class FakeS3Backend:
    def __init__(self):
        self.objects = {}
        self.list_calls = []
        self.bodies = []

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = FakeBody(self.objects[(Bucket, Key)]["Body"])
        self.bodies.append(body)
        return {"Body": body}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        stored = self.objects[(Bucket, Key)]
        return {
            "ETag": stored.get("ETag", "etag"),
            "ContentLength": len(stored["Body"]),
            "ContentType": stored.get("ContentType"),
            "Metadata": stored.get("Metadata", {}),
        }

    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None, MaxKeys=None):
        self.list_calls.append({"Bucket": Bucket, "Prefix": Prefix, "ContinuationToken": ContinuationToken, "MaxKeys": MaxKeys})
        contents = [
            {"Key": key, "Size": len(stored["Body"]), "ETag": stored.get("ETag", "etag")}
            for (bucket, key), stored in sorted(self.objects.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        start = int(ContinuationToken or 0)
        end = start + MaxKeys if MaxKeys else len(contents)
        page = contents[start:end]
        response = {"Contents": page, "IsTruncated": end < len(contents)}
        if response["IsTruncated"]:
            response["NextContinuationToken"] = str(end)
        return response

    def copy_object(self, Bucket, Key, CopySource, ContentType=None):
        source_bucket = CopySource["Bucket"]
        source_key = CopySource["Key"]
        source = self.objects[(source_bucket, source_key)]
        self.objects[(Bucket, Key)] = {**source, "ContentType": ContentType or source.get("ContentType"), "ETag": "copy-etag"}
        return {"CopyObjectResult": {"ETag": "copy-etag"}}

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        if hasattr(Body, "read"):
            Body = Body.read()
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType, "Metadata": Metadata or {}, "ETag": "etag"}
        return {"ETag": "etag"}

    def download_file(self, Bucket, Key, Filename):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        Path(Filename).write_bytes(self.objects[(Bucket, Key)]["Body"])

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {"DeleteMarker": True}


class FakeBody:
    def __init__(self, body):
        self.body = body
        self.closed = False

    def read(self):
        return self.body

    def close(self):
        self.closed = True


def test_release_surface_has_no_unimplemented_skeleton_models():
    for name in (
        "S3DateContext",
        "S3DatetimeContext",
        "S3DateRangeContext",
        "S3DatetimeRangeContext",
        "S3WriteFileContext",
    ):
        assert not hasattr(s3_base, name)

    assert not hasattr(s3_base.S3Model, "template")


def test_s3_config_package_is_exposed_for_hydra_lerna_plugins(tmp_path):
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())
    config_root = Path(__file__).parents[1] / "config"
    assert pyproject["project"]["entry-points"]["hydra.lernaplugins"]["ccflow-s3"] == "pkg:ccflow_s3.config"
    assert not (config_root / "s3_auth").exists()
    assert not (config_root / "s3_provider").exists()
    assert (config_root / "client" / "clients" / "s3" / "cloudflare.yaml").exists()
    assert (config_root / "credentials" / "credentials" / "s3" / "cloudflare.yaml").exists()

    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - cache: s3
    - output: /outputs/s3

hydra:
    searchpath:
        - pkg://ccflow_s3.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    assert isinstance(instantiate(cfg.cache.store), S3CacheStore)
    assert isinstance(instantiate(cfg.output), S3ArtifactStore)


def test_s3_model_checks_object_existence(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "existing.json")] = {"Body": b"{}", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))

    model = S3Model(
        client=S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret")),
        mode="exists",
    )

    assert model(S3ExistsContext(bucket="bucket", object="existing.json")).value is True
    assert model(S3ExistsContext(bucket="bucket", object="missing.json")).value is False


def test_s3_model_writes_json_without_overwriting_existing(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))

    model = S3Model(
        client=S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret")),
        mode="write",
        format="json",
    )

    first = model(S3WriteDataContext(bucket="bucket", object="daily/AAA.json", data={"ticker": "AAA"}))
    second = model(S3WriteDataContext(bucket="bucket", object="daily/AAA.json", data={"ticker": "BBB"}))

    assert first.value["status"] == "written"
    assert second.value["status"] == "exists"
    assert backend.objects[("bucket", "daily/AAA.json")]["Body"] == b'{"ticker":"AAA"}'


def test_s3_model_delegates_format_conversion_to_payload_codec(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    encode_calls = []
    decode_calls = []

    def encode(self, payload):
        encode_calls.append((self.format, payload))
        return b"encoded-by-codec"

    def decode(self, payload):
        decode_calls.append((self.format, payload))
        return {"decoded": True}

    monkeypatch.setattr("ccflow_s3.base.PayloadCodec.encode", encode)
    monkeypatch.setattr("ccflow_s3.base.PayloadCodec.decode", decode)

    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    model = S3Model(client=client, mode="write", format="json")
    model(S3WriteDataContext(bucket="bucket", object="daily/AAA.json", data={"ticker": "AAA"}))
    read_result = S3Model(client=client, mode="read", format="json")(S3ReadContext(bucket="bucket", object="daily/AAA.json"))

    assert encode_calls == [("json", {"ticker": "AAA"})]
    assert decode_calls == [("json", b"encoded-by-codec")]
    assert backend.objects[("bucket", "daily/AAA.json")]["Body"] == b"encoded-by-codec"
    assert read_result.value == {"decoded": True}


def test_s3_cache_store_exposes_prefixed_object_keys_and_uris():
    store = S3CacheStore(client=S3Client(), bucket="bucket", prefix="cache")

    assert store.object_key("daily/AAA.json") == "cache/daily/AAA.json"
    assert store.uri("daily/AAA.json") == "s3://bucket/cache/daily/AAA.json"


def test_s3_model_heads_lists_and_copies_objects(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "daily/AAA.json")] = {"Body": b'{"ticker":"AAA"}', "ContentType": "application/json", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    head = S3Model(client=client, mode="head")(S3HeadContext(bucket="bucket", object="daily/AAA.json"))
    listed = S3Model(client=client, mode="list")(S3ListContext(bucket="bucket", prefix="daily/"))
    copied = S3Model(client=client, mode="copy")(
        S3CopyContext(bucket="bucket", object="archive/AAA.json", source_bucket="bucket", source_object="daily/AAA.json")
    )

    assert head.value["content_length"] == len(b'{"ticker":"AAA"}')
    assert listed.value["objects"] == [{"key": "daily/AAA.json", "size": len(b'{"ticker":"AAA"}'), "etag": "etag"}]
    assert copied.value["status"] == "copied"
    assert backend.objects[("bucket", "archive/AAA.json")]["Body"] == b'{"ticker":"AAA"}'


def test_s3_model_atomic_write_records_manifest(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    model = S3Model(
        client=S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret")),
        mode="write",
        format="json",
    )

    result = model(
        S3WriteDataContext(
            bucket="bucket",
            object="daily/AAA.json",
            data={"ticker": "AAA"},
            atomic=True,
            manifest_object="manifests/daily/AAA.json",
            row_count=1,
            producer={"model": "test"},
        )
    )

    assert result.value["status"] == "written"
    assert result.value["manifest"]["object"] == "manifests/daily/AAA.json"
    assert ("bucket", "daily/AAA.json") in backend.objects
    assert ("bucket", "manifests/daily/AAA.json") in backend.objects
    manifest = json.loads(backend.objects[("bucket", "manifests/daily/AAA.json")]["Body"])
    assert manifest["object"] == "daily/AAA.json"
    assert manifest["row_count"] == 1
    assert manifest["producer"] == {"model": "test"}


def test_s3_model_atomic_write_copy_failure_preserves_existing_target_and_skips_manifest(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "daily/AAA.json")] = {"Body": b'{"ticker":"OLD"}', "ContentType": "application/json", "ETag": "old-etag"}

    def fail_copy_object(Bucket, Key, CopySource, ContentType=None):
        raise ClientError({"Error": {"Code": "InternalError", "Message": "copy failed"}}, "CopyObject")

    backend.copy_object = fail_copy_object
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    model = S3Model(
        client=S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret")),
        mode="write",
        format="json",
    )

    with pytest.raises(ClientError):
        model(
            S3WriteDataContext(
                bucket="bucket",
                object="daily/AAA.json",
                data={"ticker": "AAA"},
                overwrite=True,
                atomic=True,
                manifest_object="manifests/daily/AAA.json",
            )
        )

    assert backend.objects[("bucket", "daily/AAA.json")]["Body"] == b'{"ticker":"OLD"}'
    assert ("bucket", "manifests/daily/AAA.json") not in backend.objects


def test_s3_model_writes_and_reads_csv_and_gzip_json(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    csv_model = S3Model(client=client, mode="write", format="csv")
    csv_model(S3WriteDataContext(bucket="bucket", object="daily/bars.csv", data=[{"ticker": "AAA", "volume": 10}, {"ticker": "BBB", "volume": 20}]))

    csv_read = S3Model(client=client, mode="read", format="csv")(S3ReadContext(bucket="bucket", object="daily/bars.csv"))

    gzip_model = S3Model(client=client, mode="write", format=["json", "gzip"])
    gzip_model(S3WriteDataContext(bucket="bucket", object="daily/AAA.json.gz", data={"ticker": "AAA"}))
    gzip_read = S3Model(client=client, mode="read", format=["gzip", "json"])(S3ReadContext(bucket="bucket", object="daily/AAA.json.gz"))

    assert csv_read.value == [{"ticker": "AAA", "volume": "10"}, {"ticker": "BBB", "volume": "20"}]
    assert backend.objects[("bucket", "daily/bars.csv")]["ContentType"] == "text/csv; charset=utf-8"
    assert decompress(backend.objects[("bucket", "daily/AAA.json.gz")]["Body"]) == b'{"ticker":"AAA"}'
    assert gzip_read.value == {"ticker": "AAA"}


def test_s3_model_read_write_context_reads_source_and_writes_output(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "raw/AAA.json")] = {"Body": b'{"ticker":"AAA"}', "ContentType": "application/json", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    result = S3Model(client=client, mode="read_write", format="json")(
        S3ReadWriteContext(
            read=S3ReadContext(bucket="bucket", object="raw/AAA.json"),
            write=S3WriteDataContext(bucket="bucket", object="normalized/AAA.json", data={"ticker": "AAA", "normalized": True}),
        )
    )

    assert result.read.value == {"ticker": "AAA"}
    assert result.write.value["status"] == "written"
    assert backend.objects[("bucket", "normalized/AAA.json")]["Body"] == b'{"normalized":true,"ticker":"AAA"}'


def test_s3_model_walks_prefix_pages_and_deletes_only_with_explicit_context(monkeypatch):
    backend = FakeS3Backend()
    for index in range(5):
        backend.objects[("bucket", f"daily/{index}.json")] = {"Body": b"{}", "ETag": f"etag-{index}"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    walked = S3Model(client=client, mode="prefix_walk")(S3PrefixWalkContext(bucket="bucket", prefix="daily/", page_size=2))
    deleted = S3Model(client=client, mode="delete")(S3DeleteContext(bucket="bucket", object="daily/0.json"))

    assert [item["key"] for item in walked.value["objects"]] == [f"daily/{index}.json" for index in range(5)]
    assert [call["ContinuationToken"] for call in backend.list_calls] == [None, "2", "4"]
    assert deleted.value["deleted"] is True
    assert ("bucket", "daily/0.json") not in backend.objects


def test_s3_session_and_client_support_aws_defaults_and_compatible_endpoints(monkeypatch):
    calls = []

    class FakeSessionFactory:
        def __init__(self, **kwargs):
            calls.append({"session": kwargs})

        def client(self, service_name, **kwargs):
            calls.append({"service_name": service_name, "client": kwargs})
            return "client"

    monkeypatch.setattr("ccflow_s3.base.Session", FakeSessionFactory)

    aws_client = S3Client(region_name="us-east-1", session=S3Session(profile_name="analytics"))
    compatible_client = S3Client(
        endpoint_url="https://s3-compatible.example.test",
        region_name="us-west-2",
        session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret", aws_session_token="token"),
    )

    assert aws_client.client == "client"
    assert compatible_client.client == "client"
    assert calls[0]["session"] == {"profile_name": "analytics", "region_name": "us-east-1"}
    assert calls[1]["client"]["endpoint_url"] is None
    assert calls[3]["client"]["endpoint_url"] == "https://s3-compatible.example.test"


def test_s3_provider_resolves_common_s3_backend_endpoints(monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_R2_ACCOUNT_ID", "account-id")
    monkeypatch.setenv("HETZNER_S3_REGION", "fsn1")
    monkeypatch.setenv("BACKBLAZE_S3_REGION", "us-west-004")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")

    assert S3Provider(name="aws", region_name="us-east-1").resolved_endpoint_url() is None
    assert S3Provider(name="aws").resolved_region_name() == "us-east-2"
    assert S3Provider(name="backblaze", region_name="us-east-005").resolved_endpoint_url() == "https://s3.us-east-005.backblazeb2.com"
    assert S3Provider(name="backblaze", region_name_env="BACKBLAZE_S3_REGION").resolved_endpoint_url() == "https://s3.us-west-004.backblazeb2.com"
    assert S3Provider(name="hetzner", region_name_env="HETZNER_S3_REGION").resolved_endpoint_url() == "https://fsn1.your-objectstorage.com"
    assert S3Provider(name="cloudflare", account_id_env="CLOUDFLARE_R2_ACCOUNT_ID").resolved_endpoint_url() == (
        "https://account-id.r2.cloudflarestorage.com"
    )
    assert S3Provider(name="cloudflare", account_id="account-id").resolved_region_name() == "auto"
    assert S3Provider(name="custom", endpoint_url="https://objects.example.test").resolved_endpoint_url() == "https://objects.example.test"

    with pytest.raises(ValueError, match="custom requires endpoint_url"):
        S3Provider(name="custom").resolved_endpoint_url()


def test_s3_client_credentials_resolve_env_profile_credentials_and_anonymous_mode(monkeypatch):
    monkeypatch.setenv("S3_TEST_KEY", "configured-key")
    monkeypatch.setenv("S3_TEST_SECRET", "configured-secret")
    monkeypatch.setenv("S3_TEST_TOKEN", "configured-token")
    monkeypatch.setenv("S3_TEST_REGION", "configured-region")

    env_credentials = S3ClientCredentials(
        mode="env",
        access_key_id_env="S3_TEST_KEY",
        secret_access_key_env="S3_TEST_SECRET",
        session_token_env="S3_TEST_TOKEN",
        region_name_env="S3_TEST_REGION",
    )
    profile_credentials = S3ClientCredentials(mode="profile", profile_name="analytics", region_name="us-east-1")
    nested_credentials = S3ClientCredentials(mode="credentials", credentials=APIKeySecretCredentials(api_key="key", secret_key="secret"))

    assert env_credentials.session_kwargs() == {
        "aws_access_key_id": "configured-key",
        "aws_secret_access_key": "configured-secret",
        "aws_session_token": "configured-token",
        "region_name": "configured-region",
    }
    assert profile_credentials.session_kwargs() == {"profile_name": "analytics", "region_name": "us-east-1"}
    assert nested_credentials.session_kwargs() == {"aws_access_key_id": "key", "aws_secret_access_key": "secret"}
    assert S3ClientCredentials(mode="anonymous").session_kwargs(region_name="us-east-1") == {}
    assert S3ClientCredentials(mode="anonymous").is_anonymous is True


def test_s3_client_uses_provider_credentials_and_anonymous_unsigned_config(monkeypatch):
    calls = []

    class FakeSessionFactory:
        def __init__(self, **kwargs):
            calls.append({"session": kwargs})

        def client(self, service_name, **kwargs):
            calls.append({"service_name": service_name, "client": kwargs})
            return "client"

    monkeypatch.setattr("ccflow_s3.base.Session", FakeSessionFactory)

    cloudflare_client = S3Client(
        provider=S3Provider(name="cloudflare", account_id="account-id"),
        credentials=S3ClientCredentials(mode="access_key", access_key_id="key", secret_access_key="secret"),
    )
    anonymous_client = S3Client(
        provider=S3Provider(name="custom", endpoint_url="https://objects.example.test", region_name="us-east-1"),
        credentials=S3ClientCredentials(mode="anonymous"),
    )

    assert cloudflare_client.client == "client"
    assert anonymous_client.client == "client"
    assert calls[0]["session"] == {"aws_access_key_id": "key", "aws_secret_access_key": "secret", "region_name": "auto"}
    assert calls[1]["client"]["endpoint_url"] == "https://account-id.r2.cloudflarestorage.com"
    assert calls[1]["client"]["region_name"] == "auto"
    assert calls[2]["session"] == {}
    assert calls[3]["client"]["endpoint_url"] == "https://objects.example.test"
    assert calls[3]["client"]["config"].signature_version is UNSIGNED


def test_s3_client_and_credentials_hydra_groups_compose_with_cache_and_output(tmp_path, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_R2_ACCOUNT_ID", "account-id")
    monkeypatch.setenv("CLOUDFLARE_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "secret-key")

    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - client: /clients/s3/cloudflare
    - credentials: /credentials/s3/cloudflare
    - cache: s3
    - output: /outputs/s3

hydra:
    searchpath:
        - pkg://ccflow_s3.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    cache_store = instantiate(cfg.cache.store)
    output = instantiate(cfg.output)

    assert isinstance(cache_store.client.provider, S3Provider)
    assert isinstance(output.client.credentials, S3ClientCredentials)
    assert isinstance(instantiate(cfg.credentials.s3.cloudflare), S3ClientCredentials)
    assert output.client.resolved_endpoint_url() == "https://account-id.r2.cloudflarestorage.com"
    assert output.client.credentials.session_kwargs()["aws_access_key_id"] == "access-key"
    assert cache_store.client.resolved_region_name() == "auto"


def test_s3_session_accepts_generic_key_secret_credentials(monkeypatch):
    monkeypatch.setenv("S3_TEST_KEY", "configured-key")
    monkeypatch.setenv("S3_TEST_SECRET", "configured-secret")

    session = S3Session(credentials=APIKeySecretCredentials(api_key_env="S3_TEST_KEY", secret_key_env="S3_TEST_SECRET"))

    assert session._session_kwargs() == {"aws_access_key_id": "configured-key", "aws_secret_access_key": "configured-secret"}
    assert S3Session(credentials=S3Credentials(api_key="key", secret_key="secret"))._session_kwargs() == {
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
    }
    assert S3Session(
        credentials=APIKeySecretCredentials(api_key="credential-key", secret_key="credential-secret"),
        aws_access_key_id="explicit-key",
        aws_secret_access_key="explicit-secret",
    )._session_kwargs() == {"aws_access_key_id": "explicit-key", "aws_secret_access_key": "explicit-secret"}


def test_s3_cache_adapter_uses_s3_objects(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    cache = S3CacheStore(client=client, bucket="bucket", prefix="cache")
    cache.put_bytes("daily/AAA", b'{"ticker":"AAA"}', content_type="application/json")

    assert cache.exists("daily/AAA") is True
    assert cache.get_bytes("daily/AAA") == b'{"ticker":"AAA"}'


def test_s3_cache_store_works_with_generic_cache_models(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    store = S3CacheStore(client=client, bucket="bucket", prefix="cache")
    put_model = CachePutModel(store=store, format="json")
    get_model = CacheGetModel(store=store, format="json")

    put_result = put_model(CachePutContext(key="daily/AAA", payload={"ticker": "AAA"}, dataset="stocks", stage="extract"))
    get_result = get_model(CacheGetContext(key="daily/AAA", dataset="stocks", stage="extract"))

    assert put_result.status == "written"
    assert put_result.artifact.uri == "s3://bucket/cache/daily/AAA.json"
    assert backend.objects[("bucket", "cache/daily/AAA.json")]["ContentType"] == "application/json"
    assert get_result.status == "hit"
    assert get_result.payload == {"ticker": "AAA"}


def test_s3_artifact_store_implements_generic_artifact_contract(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "outputs/existing.json")] = {"Body": b"{}", "ContentType": "application/json", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    store = S3ArtifactStore(client=client, bucket="bucket", prefix="outputs")

    write_model = ArtifactWriteModel(store=store)
    planned = write_model(
        {
            "key": "planned.json",
            "payload": b"{}",
            "media_type": "application/json",
            "dataset": "sample_records",
            "dry_run": True,
        }
    )
    written = write_model(
        {
            "key": "new.json",
            "payload": b"{}",
            "media_type": "application/json",
            "dataset": "sample_records",
            "metadata": {"run": "test"},
        }
    )
    existing = write_model({"key": "existing.json", "payload": b"{}", "media_type": "application/json", "dataset": "sample_records"})

    assert store.artifact_uri("new.json") == "s3://bucket/outputs/new.json"
    assert planned.status == "planned"
    assert written.status == "written"
    assert written.artifact.uri == "s3://bucket/outputs/new.json"
    assert existing.status == "exists"
    assert store.read("existing.json") == b"{}"
    assert backend.bodies[-1].closed is True
    assert store.get_bytes("existing.json") == b"{}"
    assert backend.bodies[-1].closed is True
    with pytest.raises(ClientError):
        store.read("missing.json")
    assert backend.objects[("bucket", "outputs/new.json")]["Body"] == b"{}"
    assert backend.objects[("bucket", "outputs/new.json")]["Metadata"] == {"run": "test"}
    assert store.list_keys() == ["existing.json", "new.json"]
    assert store.list_keys("existing") == ["existing.json"]


def test_s3_artifact_store_publishes_from_temp_key(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "outputs/tmp/final.json")] = {"Body": b"{}", "ContentType": "application/json", "ETag": "tmp-etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    store = S3ArtifactStore(client=client, bucket="bucket", prefix="outputs")

    result = store.publish("final.json", source_key="tmp/final.json")

    assert result["status"] == "published"
    assert result["object"] == "outputs/final.json"
    assert backend.objects[("bucket", "outputs/final.json")]["Body"] == b"{}"


def test_s3_artifact_store_lists_every_inventory_page(monkeypatch):
    class PagedBackend(FakeS3Backend):
        def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None, MaxKeys=None):
            return super().list_objects_v2(Bucket, Prefix, ContinuationToken, MaxKeys=1)

    backend = PagedBackend()
    backend.objects[("bucket", "outputs/daily/AAA.json")] = {"Body": b"{}"}
    backend.objects[("bucket", "outputs/daily/BBB.json")] = {"Body": b"{}"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    store = S3ArtifactStore(client=client, bucket="bucket", prefix="outputs")

    assert store.list_keys("daily") == ["daily/AAA.json", "daily/BBB.json"]
    assert len(backend.list_calls) == 2


def test_s3_artifact_store_writes_local_file(monkeypatch, tmp_path):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    store = S3ArtifactStore(client=client, bucket="bucket", prefix="outputs")
    source_path = tmp_path / "daily.parquet"
    source_path.write_bytes(b"parquet-bytes")

    result = store.write_file("daily.parquet", source_path, media_type="application/vnd.apache.parquet", metadata={"dataset": "sample"})

    assert result["status"] == "written"
    assert result["size"] == len(b"parquet-bytes")
    assert backend.objects[("bucket", "outputs/daily.parquet")]["Body"] == b"parquet-bytes"
    assert backend.objects[("bucket", "outputs/daily.parquet")]["ContentType"] == "application/vnd.apache.parquet"
    assert backend.objects[("bucket", "outputs/daily.parquet")]["Metadata"] == {"dataset": "sample"}


def test_s3_artifact_store_materializes_file_without_reading_bytes(monkeypatch, tmp_path):
    backend = FakeS3Backend()
    backend.objects[("bucket", "outputs/daily.csv.gz")] = {"Body": b"compressed-bars"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))
    store = S3ArtifactStore(client=client, bucket="bucket", prefix="outputs")
    output_path = tmp_path / "raw" / "daily.csv.gz"

    result = store.read_file("daily.csv.gz", output_path)

    assert result["status"] == "materialized"
    assert result["size"] == len(b"compressed-bars")
    assert output_path.read_bytes() == b"compressed-bars"
    assert backend.bodies == []


def test_s3_atomic_write_does_not_publish_manifest_when_copy_fails(monkeypatch):
    class FailingCopyBackend(FakeS3Backend):
        def copy_object(self, Bucket, Key, CopySource, ContentType=None):
            raise ClientError({"Error": {"Code": "InternalError"}}, "CopyObject")

    backend = FailingCopyBackend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    model = S3Model(client=client, mode="write", format="json")

    with pytest.raises(ClientError):
        model(
            S3WriteDataContext(
                bucket="bucket",
                object="daily/AAA.json",
                data={"ticker": "AAA"},
                atomic=True,
                temp_object="tmp/daily/AAA.json",
                manifest_object="manifests/daily/AAA.json",
            )
        )

    assert ("bucket", "tmp/daily/AAA.json") in backend.objects
    assert ("bucket", "daily/AAA.json") not in backend.objects
    assert ("bucket", "manifests/daily/AAA.json") not in backend.objects


def test_get_object_bytes_retries_broken_stream(monkeypatch):
    from botocore.exceptions import ResponseStreamingError

    monkeypatch.setattr(s3_base, "sleep", lambda _: None)
    calls = []

    class FlakyBody:
        def read(self):
            raise ResponseStreamingError(error=OSError("Connection broken: IncompleteRead"))

        def close(self):
            return None

    class GoodBody:
        def read(self):
            return b"payload"

        def close(self):
            return None

    class FakeBoto:
        def get_object(self, **kwargs):
            calls.append(kwargs)
            return {"Body": FlakyBody() if len(calls) < 3 else GoodBody()}

    assert s3_base._get_object_bytes(FakeBoto(), Bucket="b", Key="k") == b"payload"
    assert len(calls) == 3


def test_get_object_bytes_raises_after_exhausting_retries(monkeypatch):
    from botocore.exceptions import ResponseStreamingError

    monkeypatch.setattr(s3_base, "sleep", lambda _: None)
    calls = []

    class FlakyBody:
        def read(self):
            raise ResponseStreamingError(error=OSError("Connection broken"))

        def close(self):
            return None

    class FakeBoto:
        def get_object(self, **kwargs):
            calls.append(kwargs)
            return {"Body": FlakyBody()}

    with pytest.raises(ResponseStreamingError):
        s3_base._get_object_bytes(FakeBoto(), Bucket="b", Key="k")
    assert len(calls) == 5
