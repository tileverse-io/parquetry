# parquetry-dependencies

Third-party dependency BOM. Pins the versions of every external library parquetry's runtime modules consume; individual module POMs then declare dependencies without `<version>`.

## What it does

- Single `<dependencyManagement>` section listing every third-party artifact parquetry depends on, version-pinned via top-level `<properties>` in the parent POM.
- Imported as a `<scope>import</scope>` BOM by every parquetry module that declares third-party dependencies - `parquetry-io`, `parquetry-format`, `parquetry-core`, the integration adapters, and the internal build modules.
- Re-imports upstream BOMs where they exist (JUnit, Testcontainers); version coordination then follows the upstream cadence.

Currently manages: Jackson 3 (`tools.jackson.core`), OpenTelemetry API, aircompressor-v3, Brotli `dec`, SLF4J API, Error Prone annotations, JSpecify, JTS 1.20.0, Lombok, JUnit 5, AssertJ, Testcontainers, parquet-avro (test-only oracle).

tileverse-storage versions are deliberately **not** managed here: the `parquetry-tileverse-storage` adapter is the one module that depends on tileverse-storage, and it imports the upstream tileverse BOM itself, keeping that coupling out of the rest of the reactor.

## Where it fits

```
                  +-----------------------------+
                  | parquetry-dependencies      |  <- you are here
                  |  (POM BOM, no jar)          |
                  +-----------------------------+
                              ^
                              | <import>
                  +--------+--------+---------+
                  |        |        |         |
            parquetry-io  -format  -core   integrations/*
```

## What it is not

This BOM only constrains **third-party** dependencies. Versions for parquetry's own modules (`parquetry-format`, `parquetry-core`, ...) are constrained by [`parquetry-bom`](../parquetry-bom/) - downstream consumers import both.

## Packaging

POM-only (`<packaging>pom</packaging>`). Skips JAR generation via `maven-jar-plugin`.
