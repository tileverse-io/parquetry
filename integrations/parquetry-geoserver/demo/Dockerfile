# GeoServer 3.0 with the parquetry GeoParquet plugin, on Java 25 with --enable-preview.
#
# Stock GeoServer images run Java 17/21 and cannot load the parquetry classes (they carry
# preview bytecode). This image takes the official platform-independent GeoServer binary
# (self-contained Jetty + exploded webapp), drops the parquetry plugin into WEB-INF/lib the
# standard GeoServer way, and runs it on a Java 25 JVM.
#
# The plugin zip (build/plugin.zip) is produced on the host by the Makefile
# (./mvnw -Passembly package on parquetry-geoserver); this image only unzips it in place.

FROM eclipse-temurin:25

ARG GEOSERVER_VERSION=3.0.0

# Proven Java 25 runtime options from GeoServer Cloud's images. Beyond parquetry's own
# --enable-preview / --enable-native-access, GeoServer needs the java.desktop, java.naming
# and java.net.http opens. UseSVE=0 works around an Apple-M4 + Docker-on-macOS SIGILL;
# UseAVX=2 keeps any AOT cache portable across amd64 hosts.
ENV DEFAULT_JAVA_TOOL_OPTIONS="\
--enable-preview \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.text=ALL-UNNAMED \
--add-opens=java.desktop/java.awt.font=ALL-UNNAMED \
--add-opens=java.desktop/sun.awt.image=ALL-UNNAMED \
--add-opens=java.desktop/sun.java2d.pipe=ALL-UNNAMED \
--add-opens=java.naming/com.sun.jndi.ldap=ALL-UNNAMED \
--add-opens=java.net.http/jdk.internal.net.http=ALL-UNNAMED \
--enable-native-access=ALL-UNNAMED \
-Djava.awt.headless=true \
-XX:MaxRAMPercentage=80 \
-XX:InitialRAMPercentage=30 \
-XX:+UseCompressedOops \
-XX:+UseCompactObjectHeaders \
-XX:+UseStringDeduplication \
-XX:+IgnoreUnrecognizedVMOptions \
-XX:UseSVE=0 \
-XX:UseAVX=2"
ENV JAVA_TOOL_OPTIONS="${DEFAULT_JAVA_TOOL_OPTIONS}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip curl \
    && rm -rf /var/lib/apt/lists/*

# The platform-independent binary unzips with no top-level directory; unpack it straight
# into /opt/geoserver (bin/, webapps/geoserver/, data_dir/, start.jar, ...).
#
# The bundled marlin-*.jar is dropped: startup.sh would inject it via --patch-module java.desktop,
# but this old Marlin references sun.security.action.GetPropertyAction, removed in JDK 24+ with the
# Security Manager, which breaks all WMS rendering. With no marlin jar, startup.sh falls back to the
# JDK's own (Java-25-compatible) Marlin renderer.
RUN curl -fSL --retry 4 --retry-delay 5 --retry-all-errors -o /tmp/geoserver.zip \
      "https://downloads.sourceforge.net/project/geoserver/GeoServer/${GEOSERVER_VERSION}/geoserver-${GEOSERVER_VERSION}-bin.zip" \
    && unzip -q /tmp/geoserver.zip -d /opt/geoserver \
    && rm /tmp/geoserver.zip \
    && rm -f /opt/geoserver/webapps/geoserver/WEB-INF/lib/marlin-*.jar \
    && rm -rf /opt/geoserver/data_dir

# Install the plugin the standard GeoServer way: unzip its jars into the webapp's WEB-INF/lib.
COPY build/plugin.zip /tmp/plugin.zip
RUN unzip -o -q /tmp/plugin.zip -d /opt/geoserver/webapps/geoserver/WEB-INF/lib \
    && rm /tmp/plugin.zip

# Bake the preconfigured data directory (workspace + GeoParquet store + published layer) and the
# sample dataset. The container then serves a live WMS/WFS layer on first boot with no manual steps.
# The data directory references the dataset at file:///data/countries.parquet.
COPY geoserver-data /opt/geoserver/data_dir
COPY data /data

ENV GEOSERVER_DATA_DIR=/opt/geoserver/data_dir
EXPOSE 8080
WORKDIR /opt/geoserver

ENTRYPOINT ["/opt/geoserver/bin/startup.sh"]
