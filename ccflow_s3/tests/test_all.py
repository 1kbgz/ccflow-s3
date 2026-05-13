import json
from gzip import decompress

import pytest
from botocore.exceptions import ClientError
from ccflow_etl import CheckpointRecord

from ccflow_s3 import (
    S3CacheStore,
    S3CheckpointStore,
    S3Client,
    S3CopyContext,
    S3DeleteContext,
    S3ExistsContext,
    S3HeadContext,
    S3ListContext,
    S3Model,
    S3PrefixWalkContext,
    S3ReadContext,
    S3ReadWriteContext,
    S3Session,
    S3WriteDataContext,
)


class FakeS3Backend:
    def __init__(self):
        self.objects = {}
        self.list_calls = []

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[(Bucket, Key)]["Body"])}

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

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType, "ETag": "etag"}
        return {"ETag": "etag"}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {"DeleteMarker": True}


class FakeBody:
    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body


def test_all():
    assert True


def test_s3_model_checks_object_existence(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "existing.json")] = {"Body": b"{}", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))

    model = S3Model(client=S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret")), mode="exists")

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


def test_s3_model_heads_lists_and_copies_objects(monkeypatch):
    backend = FakeS3Backend()
    backend.objects[("bucket", "daily/AAA.json")] = {"Body": b'{"ticker":"AAA"}', "ContentType": "application/json", "ETag": "etag"}
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    head = S3Model(client=client, mode="head")(S3HeadContext(bucket="bucket", object="daily/AAA.json"))
    listed = S3Model(client=client, mode="list")(S3ListContext(bucket="bucket", prefix="daily/"))
    copied = S3Model(client=client, mode="copy")(S3CopyContext(bucket="bucket", object="archive/AAA.json", source_bucket="bucket", source_object="daily/AAA.json"))

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


def test_s3_model_read_write_context_reads_source_and_writes_destination(monkeypatch):
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
    assert backend.objects[("bucket", "normalized/AAA.json")]["Body"] == b'{"ticker":"AAA","normalized":true}'


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


def test_s3_cache_and_checkpoint_adapters_use_s3_objects(monkeypatch):
    backend = FakeS3Backend()
    monkeypatch.setattr(S3Client, "client", property(lambda self: backend))
    client = S3Client(endpoint_url="https://s3.example.test", session=S3Session(aws_access_key_id="key", aws_secret_access_key="secret"))

    cache = S3CacheStore(client=client, bucket="bucket", prefix="cache")
    cache.put_json("daily/AAA", {"ticker": "AAA"})

    checkpoint = S3CheckpointStore(client=client, bucket="bucket", prefix="checkpoints")
    record = checkpoint.mark_succeeded("daily/AAA", metadata={"rows": 1})

    assert cache.exists("daily/AAA") is True
    assert cache.get_json("daily/AAA") == {"ticker": "AAA"}
    assert isinstance(record, CheckpointRecord)
    assert checkpoint.should_skip("daily/AAA") is True
    assert checkpoint.get("daily/AAA").metadata == {"rows": 1}


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
