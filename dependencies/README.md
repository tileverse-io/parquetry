# parquetry-dependencies

Third-party dependency BOM. Pins the versions of every external library parquetry's runtime modules consume, so individual module POMs declare dependencies without `<version>`.

## What it does

- Single `<dependencyManagement>` section listing every third-party artifact parquetry depends on, version-pinned via top-level `<properties>` in the parent POM.
- Imported as a `<scope>import</scope>` BOM by `parquetry-core`, `parquetry-geo-jts`, and `parquetry-coverage-report`.
- Re-imports upstream BOMs where they exist (JUnit, Testcontainers) so version coordination follows the upstream cadence.

Currently manages: tileverse-storage-core / -s3, Jackson 3 (`tools.jackson.core`), OpenTelemetry API, aircompressor-v3, Brotli `dec`, JTS 1.20.0, JUnit 5, AssertJ, Testcontainers, parquet-avro (test-only oracle).

## Where it fits

```
                  +-----------------------------+
                  | parquetry-dependencies      |  <- you are here
                  |  (POM BOM, no jar)          |
                  +-----------------------------+
                              ^
                              | <import>
                  +-----------+---------+-----+
                  |           |         |     |
            parquetry-core  -geo-jts  -coverage-report
```

## What it is not

This BOM only constrains **third-party** dependencies. Versions for parquetry's own modules (`parquetry-format`, `parquetry-core`, ...) are constrained by [`parquetry-bom`](../parquetry-bom/) - downstream consumers import both.

## Packaging

POM-only (`<packaging>pom</packaging>`). Skips JAR generation via `maven-jar-plugin`.
