# GitHub Actions workflows

CI and publishing for parquetry. All builds run on **Java 25** (`temurin`); the
`--enable-preview` and native-access flags come from `.mvn/jvm.config`, picked up
by `./mvnw` on every OS. Every checkout that compiles or tests uses
`submodules: recursive`, because the test corpora (`apache/parquet-testing`,
`opengeospatial/geoparquet`) are git submodules the tests read.

## `pr-validation.yml`

Checks pull requests and pushes to `main` (ignoring `docs/**` and `**.md`).
Three jobs:

- **lint** (ubuntu): `make lint`, which runs `./mvnw -Pqa validate`. The build
  binds `spotless:apply` to the `validate` phase, and it runs before the
  `spotless:check` added by the `qa` profile. Spotless formats the Java sources,
  sorts the POMs and writes the license headers, and the check then passes. The
  job does not fail on an unformatted file; it fails when Spotless cannot
  process a file, such as a Java file that does not parse.
- **build** (ubuntu + macOS + Windows): `./mvnw verify` (unit + integration
  tests). The Linux leg adds `-Pcoverage` and uploads the JaCoCo aggregate
  report; all legs upload surefire/failsafe reports.
- **pr-validation-complete**: the single required status check. Mark this job as
  the required check in branch protection.

Needs no secrets.

## `benchmarks-smoke.yml`

Builds the JMH benchmarks runner (`make build-benchmarks`) and runs it once with
tiny data (`make run-benchmarks-smoke`) as a sanity check, no measurement. Runs on
the same events as PR validation but as its own workflow, because the runner is
much slower than Build & Test and would otherwise hold the validation run open and
delay reviewing failures. Needs no secrets.

## `publish-snapshot.yml`

Runs on a push to `main` (ignoring `docs/**` and `**.md`), and on manual
dispatch. It deploys the `1.0-SNAPSHOT` build to the Central snapshot repository
with `./mvnw deploy -DskipTests`. It does not wait for `Pull Request Validation`,
and it runs no tests: a commit failing the validation run still publishes its
snapshot. A push whose head commit message contains `[skip-publish]` publishes
nothing.

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

## `docs.yml`

Builds the documentation site under `docs/` and deploys it to
[parquetry.io](https://parquetry.io). It runs when a change touches `docs/**`
or the workflow itself:

- **On a pull request** it only builds. The build is strict: a link to a
  missing page or anchor fails it.
- **On a push to `main`** it builds and publishes `docs/site` to the `gh-pages`
  branch, with `parquetry.io` as the custom domain.

The build needs Docker for the diagrams, which the hosted runner provides. See
[docs/README.md](../../docs/README.md) for what the build does.

The site depends on two settings outside this repository's files:

- **DNS.** The `parquetry.io` apex has `A` and `AAAA` records pointing at
  GitHub Pages, and `www` is a `CNAME` to `tileverse-io.github.io`.
- **Repository settings, Pages.** The source is the `gh-pages` branch, the
  custom domain is `parquetry.io`, and HTTPS is enforced.

The repository variable `GOOGLE_ANALYTICS_KEY`, a Google Analytics measurement
ID, adds analytics to the deployed pages. Pull request builds never get it.
Without it, the pages have no analytics.

## Required repository secrets

Only the two publish workflows need these:

| Secret | Purpose |
| --- | --- |
| `GPG_PRIVATE_KEY` | Armored private key used to sign artifacts. |
| `GPG_PASSPHRASE` | Passphrase for that key. |
| `CENTRAL_USERNAME` | Central Portal user token name. |
| `CENTRAL_TOKEN` | Central Portal user token secret. |
