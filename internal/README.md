# internal

Build- and development-only modules. None are published to Maven Central: each inherits `maven.deploy.skip=true` from
the `parquetry-internal` parent POM and is listed in the root `central-publishing-maven-plugin` `excludeArtifacts`.
The directory grouping is independent of the Maven coordinates.

| Module | Purpose |
|---|---|
| `parquetry-testkit` | Bundles the test corpora into a jar (`TestCorpus` extracts them into a temp dir): the `apache/parquet-testing`, `opengeospatial/geoparquet`, and `geoparquet/geoparquet-testing` git submodules, plus vendored goldens (`avro-reference`, the `jatorre/iceberg-geo-testbed` Iceberg fixtures, and parquetry's own purpose-built fixtures). Test-scope dependency of most modules. |
| `parquetry-benchmarks` | JMH benchmarks; the shaded runner is built only under `-Pbenchmarks`. |
| `parquetry-probes` | Read-comparison probes (parquetry vs parquet-java vs DuckDB) used to measure the read path. |
| `parquetry-coverage-report` | Aggregate JaCoCo coverage report across the reactor (`-Pcoverage`). |
