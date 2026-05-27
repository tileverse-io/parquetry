# parquetry-geoserver

A thin GeoServer plugin that registers the read-only GeoParquet `DataStore`
(from `parquetry-geotools`) as a vector store type in the GeoServer UI and REST
configuration.

The module ships only Spring wiring; the data-access code lives in
`parquetry-geotools`, and GeoServer auto-discovers its `DataStoreFactorySpi`
from that jar. The `applicationContext.xml` here declares:

- a `DataStorePanelInfo` binding `GeoParquetDataStoreFactory` to GeoServer's
  generic store edit panel (the connection form is generated from the factory's
  `Param[]`: `uri`, `namespace`, `fid`), and
- a `ModuleStatusImpl` so the plugin appears under About > Server Status >
  Modules.

## Runtime requirement

`parquetry-core` is Java 25 bytecode compiled with `--enable-preview`. The
plugin therefore loads only on a **Java 25 JVM started with `--enable-preview`**
(plus the foreign-memory native-access flags parquetry uses). A Java 17
GeoServer cannot load it. The deployment target is GeoServer Cloud on Java 25.

## Manual verification with `jetty:run`

The plugin is wired into the GeoServer web app through a `parquetry` Maven
profile in the GeoServer source tree (`src/web/app/pom.xml`).

1. Build and install parquetry locally:

   ```bash
   ./mvnw -pl :parquetry-geoserver -am install
   ```

2. From the GeoServer checkout (branch with the `parquetry` profile), run the
   web app on a Java 25 JVM:

   ```bash
   cd src/web/app
   MAVEN_OPTS="--enable-preview --enable-native-access=ALL-UNNAMED" \
     mvn jetty:run -Pparquetry
   ```

   Match the JVM arguments to parquetry's `.mvn/jvm.config`.

3. In the GeoServer UI, go to Stores > Add new store. "GeoParquet" appears in
   the vector data sources. Create a store with a `uri` pointing at a
   GeoParquet file (local path or `s3://`, `gs://`, `https://`, etc., per the
   tileverse storage backends), then publish a layer from it.

## Run embedded from an IDE (`StartGeoServer`)

`src/test/java/io/tileverse/parquetry/geoserver/StartGeoServer.java` launches the
full GeoServer web app, with this plugin on the classpath, inside an embedded
Jetty. Run it from an IDE as a Java application (Run As > Java Application) for a
quick debug loop. The test-scope `gs-web-app` + Jetty 12.1.8 dependencies host it;
none of them ship with the published plugin.

A minimal `web.xml` is bundled under `src/test/resources/webapp`. **No GeoServer
source checkout is needed** - `StartGeoServer` serves the bundled webapp and
loads GeoServer plus the plugin from the classpath. To serve a different webapp
(for example a real GeoServer source tree), override it with
`-Dgeoserver.webapp=/path/to/webapp`.

Run configuration (the JVM flags match parquetry's `.mvn/jvm.config`):

```
VM arguments:
  --enable-preview --enable-native-access=ALL-UNNAMED
```

GeoServer comes up at <http://localhost:8080/geoserver> (override the port with
`-Djetty.port=...`); type `stop` in the console to shut down. Then add a
"GeoParquet" store as in step 3 above.
