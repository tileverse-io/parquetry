# Avro reference corpus

Real Avro files copied (not submoduled) from upstream projects, for read-conformance tests.

- `weather.avro`, `weather-snappy.avro`, `weather-sorted.avro`, `weather.json`, `syncInMeta.avro`,
  `test.avro12`, `schema-tests.txt`, `messageV1/`, `interop.avsc`, `weather.avsc`: copied from
  apache/avro tag `release-1.12.0`, `share/test/data` and `share/test/schemas` (Apache License 2.0).
  `test.avro12` was written by Avro 1.2 (pre-1.3 data file format, magic `Obj\x00`);
  `schema-tests.txt` holds Parsing Canonical Form and fingerprint vectors; `messageV1/` holds
  single-object encoding vectors.
- `weather-deflate.avro`, `weather-zstd.avro`: copied from apache/avro commit
  `54b332161524086dcb6cde8afe097097eed7f3ee` (AVRO-4172), `share/test/data` (Apache License 2.0);
  these two codecs' weather files were added upstream after the 1.12 releases.
- `interop/*.avro`: copied from https://github.com/rdblue/avro-interop (Apache License 2.0),
  cross-implementation files (c, java, php, py, ruby) written against `interop.avsc`.
- `iceberg-manifest/manifest.avro`, `iceberg-manifest/manifest-list.avro`: a real Apache Iceberg
  manifest and manifest list (uncompressed Avro OCF), copied from the jatorre/iceberg-geo-testbed
  goldens (Apache License 2.0). They exercise the reader against a real-world OCF with custom
  field-id attributes and binary lower/upper bounds; the reader stays Iceberg-agnostic.
