# parquetry-bom

Bill of materials listing every parquetry-* module at a single coordinated version. Downstream consumers import this BOM so they can depend on parquetry modules without per-module `<version>` declarations.

## What it does

Single `<dependencyManagement>` section listing each published parquetry module - currently `parquetry-format`, `parquetry-core`, `parquetry-geo-jts`, `parquetry-encryption` (placeholder), `parquetry-variant` (placeholder) - all pinned to `${project.version}` so the BOM tracks the parent's `${revision}`.

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
            parquetry-format, parquetry-core, parquetry-geo-jts, ...
```

## What it is not

This BOM only constrains **parquetry's own** module versions. Third-party dependency versions live in [`parquetry-dependencies`](../parquetry-dependencies/). Downstream consumers typically import both.

## Packaging

POM-only (`<packaging>pom</packaging>`). Skips JAR generation via `maven-jar-plugin`.
