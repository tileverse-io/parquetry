# parquetry-variant (placeholder)

Empty module reserved for Parquet's `Variant` logical type.

## Status

Placeholder. No production code yet. The module exists today so the reactor's shape is final and the `LogicalType.VariantStub` placeholder in `parquetry-format` has a clear home for its eventual payload implementation.

## What it will do

- Implement the [Parquet Variant binary format](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md): the schema-less, self-describing value container that's the lingua franca for semi-structured columns in modern Parquet writers.
- Promote `LogicalType.VariantStub` to a real `Variant(VariantType)` record carrying the variant schema.
- Provide a navigator API (`VariantValue` with typed accessors) consumers can use instead of the raw `Object` returned by `ParquetRecord.get()` for variant columns.

## Why a placeholder today

`LogicalType` is a closed sealed interface that already permits `VariantStub`. Pre-declaring this module means the variant work can ship `Variant(VariantType)` as a plain rename + payload, with no `permits`-list churn.

## Dependencies

None today.
