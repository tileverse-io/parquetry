# Contribute

You want to fix a bug or add a feature to Parquetry. This page lists the conventions followed by a change. [Build from source](build.md) sets up the build first.

## Formatting

Spotless formats the code with palantir-java-format, and sorts the elements of the POM files. A build formats the sources in its `validate` phase, before it compiles them. `./mvnw spotless:apply` formats them without building. Build before you commit, and commit the formatted files.

## License headers

Each Java file starts with the Apache License 2.0 header used by the project. Spotless adds it to a new file during the build.

## Tests

The name of a test class decides when it runs:

- A class named `*Test` holds unit tests, run by `./mvnw test`.
- A class named `*IT` holds integration tests, run by `./mvnw verify`.

Tests use JUnit Jupiter and AssertJ.

## Commit messages

The title of a commit follows [Conventional Commits](https://www.conventionalcommits.org/): a type, an optional scope, and a short description.

| Type | For |
| :--- | :--- |
| `feat` | A new capability |
| `fix` | A bug fix |
| `perf` | A performance improvement |
| `refactor` | A change of structure, with the same behavior |
| `test` | Tests only |
| `docs` | Documentation only |
| `chore` | Build and maintenance |

For example: `fix: wrong rows from filters when the offset index misplaces pages`, or `feat(geotools): cache remote byte ranges by default`.

## Use of AI tools

Contributions written with the help of AI tools follow the Tileverse policy, published at [tileverse.io/ai](https://tileverse.io/ai/).
