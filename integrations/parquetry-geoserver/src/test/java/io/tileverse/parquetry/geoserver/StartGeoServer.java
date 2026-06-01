/*
 * Copyright (c) 2026 Multivers.io
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 */
package io.tileverse.parquetry.geoserver;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Embedded GeoServer launcher for IDE development. Runs the full GeoServer web application with the parquetry-geoserver
 * plugin on the classpath, which makes the "GeoParquet" store type available without deploying a war. Run it from an
 * IDE as a Java application (Run As &gt; Java Application).
 *
 * <p>The plugin and GeoServer load only on a <b>Java 25 JVM started with {@code --enable-preview}</b> (plus parquetry's
 * foreign-memory native-access flags). Set these VM arguments in the run configuration:
 *
 * <pre>
 *   --enable-preview --enable-native-access=ALL-UNNAMED
 * </pre>
 *
 * <p>A minimal {@code web.xml} is bundled under {@code src/test/resources/webapp}. No GeoServer source checkout is
 * needed. To serve a different webapp (for example a real GeoServer source tree), override it with
 * {@code -Dgeoserver.webapp=/path/to/webapp} (the directory containing {@code WEB-INF/web.xml}). Type {@code stop} in
 * the console to shut down.
 */
public final class StartGeoServer {

    private StartGeoServer() {}

    @SuppressWarnings("java:S106") // a console launcher; System.out is the intended user channel
    public static void main(String... args) throws Exception {

        Server server = new Server();
        HttpConfiguration httpConfig = new HttpConfiguration();
        ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        http.setPort(Integer.getInteger("jetty.port", 8080));
        http.setIdleTimeout(60L * 60L * 1000L);
        server.setConnectors(new Connector[] {http});

        WebAppContext webAppContext = webAppContext();
        server.setHandler(webAppContext);

        server.start();
        IO.println("GeoServer running at http://localhost:%d/geoserver".formatted(http.getPort()));
    }

    private static WebAppContext webAppContext() throws URISyntaxException {
        File webapp = resolveWebapp();
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/geoserver");
        webAppContext.setWar(webapp.getAbsolutePath());
        webAppContext.setTempDirectory(new File("target/geoserver-work"));
        webAppContext.setMaxFormContentSize(5 * 1024 * 1024);
        webAppContext.setMaxFormKeys(2000);
        return webAppContext;
    }

    private static File resolveWebapp() throws URISyntaxException {
        URL bundled =
                Objects.requireNonNull(StartGeoServer.class.getClassLoader().getResource("webapp/WEB-INF/web.xml"));
        File webInf = new File(bundled.toURI()).getParentFile();
        return webInf.getParentFile();
    }
}
