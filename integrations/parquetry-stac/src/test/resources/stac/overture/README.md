# Overture collections.parquet (vendored test artifact)

- Source: https://stac.overturemaps.org/2026-07-22.0/collections.parquet
- Release: Overture Maps 2026-07-22.0
- Retrieved: 2026-08-10
- Size / cksum: 232,141 bytes / 1325068109
- Shape: stac-geoparquet item-table, one row per STAC item (974 items,
  15 collections), footer KV `stac-geoparquet` = {"version": "1.0.0"}
  (no embedded collections mapping), `geo` = GeoParquet 1.1.0.
- Purpose: integration oracle for GeoParquetStacReader spec compliance.
  The asset hrefs point at Overture's public AWS/Azure endpoints; tests
  read only this file, never the referenced data.
- License: the file is catalog metadata for collections licensed
  ODbL-1.0, CC-BY-4.0, CC0-1.0, and "other" (see each embedded
  collection's license field on stac.overturemaps.org).
- Redistribution: the Overture Maps Foundation publishes this file openly
  at the URL above; it is vendored here unmodified as a test fixture,
  with attribution: (c) Overture Maps Foundation.
