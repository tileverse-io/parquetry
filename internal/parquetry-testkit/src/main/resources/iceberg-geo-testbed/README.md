# iceberg-geo-testbed corpus

Vendored Apache Iceberg test fixtures for the native Iceberg read path: a static
catalog (`metadata.json` + manifest-list + manifest Avro) over ten single-region
Parquet data files, with per-file lower/upper bounds in the manifest. Built to
exercise file-level pruning, including geometry bbox pruning.

## Provenance

- Source: https://github.com/jatorre/iceberg-geo-testbed
- Commit: `59f47a71f641f14994f54e279791e8188edc723f`
- License: Apache-2.0 (see `LICENSE` in this directory).
- The upstream repo `.gitignore`s its generated fixtures. The generator is
  pinned as a git submodule at `../../fixtures-gen/iceberg-geo-testbed` and these
  goldens are produced from it by `../../fixtures-gen/regenerate.py`.

### Regenerating (byte-reproducible)

```bash
git submodule update --init internal/parquetry-testkit/fixtures-gen/iceberg-geo-testbed
cd internal/parquetry-testkit/fixtures-gen
python3 -m venv /tmp/iceberg-fixtures-venv
source /tmp/iceberg-fixtures-venv/bin/activate
pip install -r requirements.txt
python regenerate.py
```

Regeneration is byte-identical run-to-run. The upstream Parquet data and per-file
bounds are already deterministic (each region's RNG is seeded with a hashlib
`stable_seed`, not Python's per-process `hash()`). `regenerate.py` pins the three
remaining wall-clock / entropy inputs so the `metadata.json` and Avro files also
reproduce exactly: the `snapshot-id` (`time.time`), the `table-uuid`
(`uuid.uuid4`), and the Avro OCF sync marker (`os.urandom`). It also passes the
host-independent `location_uri` (see below). Reproducing requires the pinned
toolchain in `requirements.txt` (pyarrow stamps its version into each Parquet
footer's `created_by`).

## Path portability (important for the reader harness)

The fixtures were generated with a host-independent location root. Every URI
inside `metadata.json` and the manifest Avro is rooted at:

```
file:///iceberg-geo-testbed/<fixture>/...
```

These paths do not exist on disk. A reader test must remap that root to the
directory the corpus is extracted into. Recommended harness step: extract via
`TestCorpus.extractDirectory("iceberg-geo-testbed/<fixture>", tempDir)`, then
resolve any `file:///iceberg-geo-testbed/<fixture>/` URI (in `location`, the
snapshot `manifest-list`, each manifest `manifest_path`, and each data-file
`file_path`) against `tempDir/<fixture>`.

## The fixtures

All ten data files hold 1000 synthetic rows constrained to one of ten disjoint
world regions, which makes each file's per-column bounds equal its region's bbox.
The pruning probe is a tight California window `(xmin=-125, ymin=32, xmax=-115,
ymax=42)`; only the `california` file overlaps it. A correct file-level pruner
narrows the scan to 1 of 10 files = 196 matching rows.

| Fixture | Format claimed | Geometry representation | Bound field ids | Pruning target |
|---|---|---|---|---|
| `v2_flat_columns` | V2 | flat `xmin/ymin/xmax/ymax` double columns | 2,3,4,5 (doubles) | 1/10 via numeric column bounds |
| `v2_bbox_struct` | V2 | a `bbox` struct of four doubles | struct fields | demonstrates the struct-pushdown gap (engines scan 10/10) |
| `v2_geo_convention` | V2 | flat bbox columns + a `geom_wkb` WKB column + `geo` table property | bbox column ids | 1/10 + end-to-end WKB materialization |
| `v3_geometry` | V3 | native `geometry(OGC:CRS84)` column | 2 (packed_xy geometry bound) | 1/10 via manifest geometry bounds |
| `v3_geometry_lineage` | V3 | same as `v3_geometry` plus `_row_id` / `_last_updated_sequence_number` lineage columns in the data files | 2 | row-lineage column handling |
| `v3_minimal` | V3 | no geometry; id column only | 1 (string id bound) | smallest valid V3 catalog (parse/sanity) |

## Bound byte formats (the decoder targets)

Iceberg single-value serialization, little-endian (Appendix D):
- `double` bound = 8-byte IEEE-754 LE. Example (`v2_flat_columns`, field 2 `xmin`
  of `pacific_far_west`): `00 00 00 00 00 80 66 c0` = -180.0.
- `string` bound = UTF-8 bytes (e.g. `pacific_far_west-0`).
- `v3_geometry` geometry bound, `packed_xy` encoding = 16 bytes = two LE doubles:
  lower = `(xmin, ymin)`, upper = `(xmax, ymax)`. Example lower
  `00000000008066c0 00000000000024c0` = (-180.0, -10.0), upper
  `0000000000c062c0 0000000000002440` = (-150.0, 10.0). (The generator can also
  emit a `wkb_point` geometry-bound encoding; the vendored copy uses the
  `packed_xy` default.)

## Scope notes

- Append-only: every manifest entry is `ADDED` data content; there are no delete
  manifests. A delete-manifest fail-fast fixture must be hand-built separately.
- The V3 fixtures are written by subclassing pyiceberg's V2 manifest writers
  (0.11.1 rejects native V3 writes), with a schema-override JSON so the
  `geometry` type token reaches the manifest metadata, plus the V3 snapshot
  fields (`first-row-id`, `added-rows`, `next-row-id`, empty `statistics` /
  `partition-statistics`). See the upstream `testbed/_static_catalog.py` for the
  exact shape and the `STATUS_V3.md` notes on per-field requirements.
