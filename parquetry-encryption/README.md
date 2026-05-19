# parquetry-encryption (placeholder)

Empty module reserved for Parquet Modular Encryption (PME) support.

## Status

Placeholder. No production code yet. The module exists today so the reactor's shape is final and downstream consumers can rely on a stable set of `parquetry-*` artifacts.

## What it will do

- Implement [Parquet Modular Encryption (PME)](https://github.com/apache/parquet-format/blob/master/Encryption.md): AES-GCM-V1 and AES-GCM-CTR-V1 algorithms.
- Plug into `parquetry-format`'s codec layer so encrypted file metadata, column metadata, page headers, and page data round-trip transparently.
- Wire the `DecryptionKeyRetriever` SPI hook already declared in `parquetry-core`'s `ReadOptions` to supply per-column DEKs from a KMS-backed retriever.

## Why a placeholder today

The exception hierarchy and SPI shape (`DecryptionKeyRetriever`, `EncryptionAlgorithm` sealed interface) are already in place in the other modules, so an encryption implementation can drop in later without breaking existing consumers.

## Dependencies

None today. An implementation will add `parquetry-format` plus the JCA / BouncyCastle backing for AES-GCM if needed.
