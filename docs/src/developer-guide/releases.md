# Releases

You want to know how a Parquetry version reaches Maven Central, or how to use a build of `main` before it is released. GitHub Actions workflows build, test and publish each version, and publish this site.

## Pull request checks

Pull requests and pushes to `main` run the same checks:

- **Lint**: `make lint` runs the formatting step of the build, with Spotless.
- **Build**: `./mvnw verify` on Linux, macOS and Windows, with Java 25. The Linux build also measures test coverage.

The checks skip changes that touch only `docs/` or Markdown files.

## Snapshots

A push to `main` publishes the build as a `-SNAPSHOT` version to the snapshot repository of Maven Central, `https://central.sonatype.com/repository/maven-snapshots/`. To use a snapshot, declare that repository in your build with snapshots enabled.

The publishing workflow skips the tests and does not wait for the pull request checks. A push touching only `docs/` or Markdown files publishes no snapshot, and neither does a commit whose message contains `[skip-publish]`.

## Releases

Pushing a tag whose name starts with a digit, such as `1.0.0` or `2.0-RC1`, publishes a release. The workflow builds and tests the tagged commit, publishes that exact version to Maven Central, then creates the matching GitHub release. The tag name is the version. A tag starting with another character publishes nothing.

A maintainer can also release any commit with any version by hand, from the same workflow.

The project version in the POM files is `${revision}`. The release workflow sets it from the tag. Other builds use the `-SNAPSHOT` version of `main`.

## This site

A separate workflow builds the site from `docs/` when a change touches that directory:

- On a pull request, it builds the site to check it. A link to a missing page or anchor fails the build.
- On a push to `main`, it builds the site and publishes it to [parquetry.io](https://parquetry.io).

The site follows `main`. It may describe features newer than the latest release; [Getting started](../getting-started.md) names the release.

The [README of the workflows directory](https://github.com/tileverse-io/parquetry/blob/main/.github/workflows/README.md) describes each workflow in detail.
