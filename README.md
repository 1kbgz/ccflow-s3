# ccflow-s3

ccflow models for S3

[![Build Status](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-s3/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-s3)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-s3)](https://github.com/1kbgz/ccflow-s3)
[![PyPI](https://img.shields.io/pypi/v/ccflow-s3.svg)](https://pypi.python.org/pypi/ccflow-s3)

## Overview

`ccflow-s3` provides public, domain-neutral S3 and S3-compatible storage callable models for `ccflow` workflows. It should own S3 session/client configuration, object path templating, object reads/writes, existence checks, listing, manifests, atomic publish behavior, and S3-backed cache/checkpoint adapters.

It should stay storage-focused. Dataset schemas, finance semantics, provider endpoint catalogs, and application storage conventions belong outside this package.

## Current Status

- Implemented: `S3Config`, flexible `S3Session`, `S3Client`, S3 context/result shells, object path templating, object reads, binary/text/json/CSV/gzip decoding, optional parquet hooks, object existence checks through `S3ExistsContext`, metadata reads through `S3HeadContext`, prefix listing through `S3ListContext`, paged prefix walks through `S3PrefixWalkContext`, object copies through `S3CopyContext`, explicit deletes through `S3DeleteContext`, additive `S3WriteDataContext` writes for byte/string/dict/list payloads, CSV/gzip/parquet-capable write encoding, read-write orchestration through `S3ReadWriteContext`, temp-key copy finalization for atomic-style publishes, JSON manifest writes through `S3ObjectManifest`, S3 cache/checkpoint adapters, and local fake-S3 tests with botocore error semantics.
- Partial: write mode skips existing objects unless `overwrite=True`, atomic publishes leave the temporary object in place rather than deleting it by default, and parquet support depends on optional pandas/parquet-engine availability.
- Missing: manifest readers, temporary-object cleanup policy, live provider examples, and broader moto/botocore-stub integration coverage.

## Dependency Contract

- Depends on `ccflow` and S3 client libraries.
- May implement generic `ccflow-etl` cache/checkpoint interfaces once they exist.
- Must not depend on finance packages or application-specific packages.

## Test Convention

Default tests should use botocore stubs, moto, or local synthetic fixtures. Tests requiring real S3-compatible credentials should be opt-in and skipped by default.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
