# parquetry-storage-it

Integration tests reading one Parquet file through each tileverse-storage backend, and comparing the result with a
read of the local file. Never published.

| Class | Backend | Needs Docker |
|---|---|---|
| `FileStorageReadIT` | a local directory | no |
| `HttpStorageReadIT` | Apache httpd | yes |
| `S3StorageReadIT` | LocalStack | yes |
| `AzureStorageReadIT` | Azurite | yes |
| `GcsStorageReadIT` | fake-gcs-server | yes |

`AbstractStorageReadIT` holds the tests: one read, and sixteen virtual threads reading the same object. A backend class
supplies a container URI and the `storage.*` properties addressing it; the template opens the Storage with
`StorageFactory.open(uri, properties)`, as a deployment does, never from an SDK client built by the test. The classes
needing Docker skip themselves without it.

The tests live in a module of their own to keep the cloud SDKs and Testcontainers off the test class path of
`parquetry-core`, where the parquet-java oracle pins older versions of `commons-io`, `commons-lang3`, `protobuf` and
Guava.

## Running

```bash
./mvnw -pl :parquetry-storage-it -am verify                              # the five backends
./mvnw -pl :parquetry-storage-it -am verify -Dit.test=S3StorageReadIT    # one backend
./mvnw verify -pl '!:parquetry-storage-it'                               # the reactor without them
```
