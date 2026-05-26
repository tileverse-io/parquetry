# parquetry-bom

Bill of materials listing every foundational parquetry-* module at a single coordinated version. Downstream consumers import this BOM to depend on parquetry modules without per-module `<version>` declarations.

## What it does

Single `<dependencyManagement>` section listing each foundational published parquetry module - currently `parquetry-io`, `parquetry-format`, `parquetry-core`, `parquetry-encryption` (placeholder), `parquetry-variant` (placeholder) - all pinned to `${project.version}`, which tracks the parent's `${revision}`.

## Where it fits

```
                  consumer pom.xml
                       |
                       v <import>
                +-------------------+
                | parquetry-bom     |  <- you are here
                +-------------------+
                       ^
                       | <dependency>
            parquetry-io, parquetry-format, parquetry-core, ...
```

## What it is not

This BOM only constrains **parquetry's own** module versions. The integration adapters (`parquetry-geo-jts`, `parquetry-tileverse-storage`) are published independently and are not listed here; depend on them directly with an explicit version. Third-party dependency versions live in [`parquetry-dependencies`](../parquetry-dependencies/). Downstream consumers typically import both.

## Packaging

POM-only (`<packaging>pom</packaging>`). Skips JAR generation via `maven-jar-plugin`.
