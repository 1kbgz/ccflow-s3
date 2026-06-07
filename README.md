# ccflow-s3

ccflow models for S3

[![Build Status](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-s3/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-s3/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-s3)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-s3)](https://github.com/1kbgz/ccflow-s3)
[![PyPI](https://img.shields.io/pypi/v/ccflow-s3.svg)](https://pypi.python.org/pypi/ccflow-s3)

## Overview

`ccflow-s3` provides public, domain-neutral S3 and S3-compatible storage callable models for `ccflow` workflows. It provides client and credential configuration, object reads/writes, existence checks, listing, manifests, atomic publish behavior, an S3-backed cache adapter, a generic artifact store adapter, and Hydra config groups exposed through the lerna plugin entry point.

It is storage-focused. Dataset schemas, domain-specific semantics, private bucket conventions, and application storage conventions are not part of this package.

## Current Status

- Implemented: `S3Config`, `S3Credentials`, `S3ClientCredentials`, `S3Provider`, flexible `S3Session` with direct AWS fields or `ccflow-etl` key/secret credentials, `S3Client`, client configs for AWS, Backblaze B2, Hetzner Object Storage, Cloudflare R2, and custom S3-compatible stores, shared `/credentials/s3/...` configs, anonymous unsigned access, S3 operation contexts/results, object reads, binary/text/json/CSV/gzip decoding through `ccflow-etl` `PayloadCodec`, pyarrow-backed parquet hooks, object existence checks through `S3ExistsContext`, metadata reads through `S3HeadContext`, prefix listing through `S3ListContext`, paged prefix walks through `S3PrefixWalkContext`, object copies through `S3CopyContext`, explicit deletes through `S3DeleteContext`, additive `S3WriteDataContext` writes for byte/string/dict/list payloads, codec-backed CSV/gzip/parquet write encoding, read-write orchestration through `S3ReadWriteContext`, temp-key copy finalization for atomic-style publishes, JSON manifest writes through `S3ObjectManifest`, `S3ArtifactStore` for `ccflow-etl` artifact write/publish contracts, S3 cache adapter, `cache=s3`, and `output=/outputs/s3` config groups, and local fake-S3 tests with botocore error semantics.
- Partial: write mode skips existing objects unless `overwrite=True`, atomic publishes leave the temporary object in place rather than deleting it by default, and parquet support depends on `pyarrow` through `ccflow-etl`.
- Missing: manifest readers, temporary-object cleanup policy, live provider smoke tests, and broader moto/botocore-stub integration coverage.

## Client And Credentials Config

Select a client group alongside `output=/outputs/s3` or `cache=s3`:

```bash
cc-etl +client=/clients/s3/cloudflare +output=/outputs/s3 output.bucket=my-bucket output.prefix=raw
cc-etl +client=/clients/s3/backblaze +cache=s3 cache.store.bucket=my-bucket cache.store.prefix=cache
```

Available client groups:

| Group                           | Resolution                                                                                                          |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `client=/clients/s3/aws`        | AWS S3 SDK defaults, optional `AWS_REGION` or `AWS_DEFAULT_REGION`.                                                 |
| `client=/clients/s3/backblaze`  | `BACKBLAZE_S3_ENDPOINT_URL`, or `https://s3.${BACKBLAZE_S3_REGION}.backblazeb2.com`.                                |
| `client=/clients/s3/hetzner`    | `HETZNER_S3_ENDPOINT_URL`, or `https://${HETZNER_S3_REGION}.your-objectstorage.com`.                                |
| `client=/clients/s3/cloudflare` | `CLOUDFLARE_R2_ENDPOINT_URL`, or `https://${CLOUDFLARE_R2_ACCOUNT_ID}.r2.cloudflarestorage.com` with region `auto`. |
| `client=/clients/s3/custom`     | `S3_ENDPOINT_URL` and optional `S3_REGION`.                                                                         |

Available standalone credentials groups:

| Group                                     | Resolution                                                                  |
| ----------------------------------------- | --------------------------------------------------------------------------- |
| `credentials=/credentials/s3/default`     | Boto3 default credential chain.                                             |
| `credentials=/credentials/s3/aws-profile` | `AWS_PROFILE` plus optional `AWS_REGION`.                                   |
| `credentials=/credentials/s3/aws`         | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`. |
| `credentials=/credentials/s3/backblaze`   | `BACKBLAZE_ACCESS_KEY_ID`, `BACKBLAZE_SECRET_ACCESS_KEY`.                   |
| `credentials=/credentials/s3/hetzner`     | `HETZNER_S3_ACCESS_KEY_ID`, `HETZNER_S3_SECRET_ACCESS_KEY`.                 |
| `credentials=/credentials/s3/cloudflare`  | `CLOUDFLARE_R2_ACCESS_KEY_ID`, `CLOUDFLARE_R2_SECRET_ACCESS_KEY`.           |
| `credentials=/credentials/s3/custom`      | `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, optional `S3_SESSION_TOKEN`.    |
| `credentials=/credentials/s3/anonymous`   | Unsigned public object access.                                              |

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
