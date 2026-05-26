# ccflow-s3

ccflow models for S3

[![Build Status](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-s3/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-s3)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-s3)](https://github.com/1kbgz/ccflow-s3)
[![PyPI](https://img.shields.io/pypi/v/ccflow-s3.svg)](https://pypi.python.org/pypi/ccflow-s3)

## Overview

`ccflow-s3` provides public, domain-neutral S3 and S3-compatible storage callable models for `ccflow` workflows. It provides S3 session/client configuration, object reads/writes, existence checks, listing, manifests, atomic publish behavior, S3-backed cache/checkpoint adapters, and Hydra config groups exposed through the lerna plugin entry point.

It is storage-focused. Dataset schemas, domain-specific semantics, provider endpoint catalogs, and application storage conventions are not part of this package.

## Current Status

- Implemented: `S3Config`, flexible `S3Session`, `S3Client`, S3 operation contexts/results, object reads, binary/text/json/CSV/gzip decoding through `ccflow-etl` `PayloadCodec`, pyarrow-backed parquet hooks, object existence checks through `S3ExistsContext`, metadata reads through `S3HeadContext`, prefix listing through `S3ListContext`, paged prefix walks through `S3PrefixWalkContext`, object copies through `S3CopyContext`, explicit deletes through `S3DeleteContext`, additive `S3WriteDataContext` writes for byte/string/dict/list payloads, codec-backed CSV/gzip/parquet write encoding, read-write orchestration through `S3ReadWriteContext`, temp-key copy finalization for atomic-style publishes, JSON manifest writes through `S3ObjectManifest`, S3 cache/checkpoint adapters, `cache=s3` and `checkpoint=s3` config groups, and local fake-S3 tests with botocore error semantics.
- Partial: write mode skips existing objects unless `overwrite=True`, atomic publishes leave the temporary object in place rather than deleting it by default, and parquet support depends on `pyarrow` through `ccflow-etl`.
- Missing: manifest readers, temporary-object cleanup policy, live provider examples, and broader moto/botocore-stub integration coverage.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
