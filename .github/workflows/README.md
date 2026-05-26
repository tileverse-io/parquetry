# GitHub Actions workflows

CI and publishing for parquetry. All builds run on **Java 25** (`temurin`); the
`--enable-preview` and native-access flags come from `.mvn/jvm.config`, picked up
by `./mvnw` on every OS. Every checkout that compiles or tests uses
`submodules: recursive`, because the test corpora (`apache/parquet-testing`,
`opengeospatial/geoparquet`) are git submodules the tests read.

## `pr-validation.yml`

Gates every pull request and every push to `main` (ignoring `docs/**` and
`**.md`). Three jobs:

- **lint** (ubuntu): `make lint` - Spotless, SortPOM, and license-header checks.
- **build** (ubuntu + macOS + Windows): `./mvnw verify` (unit + integration
  tests). The Linux leg adds `-Pcoverage` and uploads the JaCoCo aggregate
  report; all legs upload surefire/failsafe reports.
- **pr-validation-complete**: the single required status check. Mark this job as
  the required check in branch protection.

Needs no secrets.

## `publish-snapshot.yml`

After `Pull Request Validation` completes successfully on `main` (or on manual
dispatch), deploys the `1.0-SNAPSHOT` build to the Central snapshot repository.
Skipped when the head commit message contains `[skip-publish]`.

## `publish-release.yml`

Builds, tests, and publishes a release to Maven Central, then creates the GitHub
Release. The version is fed to Maven as `-Drevision=<version>` (the POM uses
CI-friendly `${revision}` versioning; the tag drives the published version).

Triggered by either:

- **Pushing a tag whose name starts with a digit** - `1.0-M1`, `1.0.0`,
  `2.0-RC1`. The version is the tag name verbatim. Tags that do not start with
  a digit are ignored and never trigger a publish.
- **`workflow_dispatch`** with an explicit `version` input - for releasing any
  ref or version by hand.

## Required repository secrets

Only the two publish workflows need these:

| Secret | Purpose |
| --- | --- |
| `GPG_PRIVATE_KEY` | Armored private key used to sign artifacts. |
| `GPG_PASSPHRASE` | Passphrase for that key. |
| `CENTRAL_USERNAME` | Central Portal user token name. |
| `CENTRAL_TOKEN` | Central Portal user token secret. |
