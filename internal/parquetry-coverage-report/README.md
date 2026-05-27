# coverage-report

Aggregator module that produces a single JaCoCo HTML report covering parquetry-format and parquetry-core's unit + integration test runs.

## What it does

Runs JaCoCo's `report-aggregate` goal over the per-module `jacoco.exec` and `jacoco-it.exec` files produced when the reactor builds under the `coverage` profile. Output lands at:

```
coverage-report/target/site/jacoco-aggregate/index.html
```

## Where it fits

Only joins the reactor when the `coverage` profile is active. The default `./mvnw verify` build is JaCoCo-free.

```
./mvnw -Pcoverage clean verify
                |
                v
   prepare-agent / prepare-agent-integration (per module)
                |
                v
            jacoco.exec  +  jacoco-it.exec
                |
                v
   coverage-report :: report-aggregate (verify phase)
                |
                v
   coverage-report/target/site/jacoco-aggregate/
```

## Notes

- The module is also marked `<packaging>pom</packaging>` and skips the deploy plugin - it's an internal artifact, never published.
- The aggregate spans `parquetry-format` and `parquetry-core` only. The newer `parquetry-io` module (the `ByteRangeSource` / `SegmentPool` SPIs) and the `parquetry-tileverse-storage` adapter run their own tests in-module but are not folded into this report yet; adding them is a candidate follow-up.
- Aggregated coverage sits around ~80% line / ~58% branch. Lifting that figure is follow-up work, mostly in parquetry-format's Thrift decoders.
