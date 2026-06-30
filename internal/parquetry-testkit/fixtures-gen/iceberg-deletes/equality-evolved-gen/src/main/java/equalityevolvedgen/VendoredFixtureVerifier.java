/*
 * Copyright 2026 Tileverse.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package equalityevolvedgen;

import java.io.IOException;
import java.nio.file.Path;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Reads a vendored fixture back through Iceberg's own reader to prove it still resolves and yields
 * the same live-row set after the path rewrite.
 *
 * <p>This verifier reads category as reconstructed by Iceberg from the partition tuple, which works
 * on the vendored table whose data files no longer store category. The argument is a directory whose
 * internal paths already point to the table location (the caller stages the vendored tree under that
 * location).
 */
public final class VendoredFixtureVerifier {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: VendoredFixtureVerifier <table-location>");
        }
        Path tableLocation = Path.of(args[0]);
        Table table = openTable(tableLocation);
        TreeSet<Long> liveIds = readLiveIds(table);
        System.out.println("VENDORED-LIVE-COUNT=" + liveIds.size());
        System.out.println("VENDORED-LIVE-IDS=" + liveIds);
    }

    private static Table openTable(Path tableLocation) {
        HadoopTables tables = new HadoopTables(new Configuration());
        return tables.load(tableLocation.toUri().toString());
    }

    private static TreeSet<Long> readLiveIds(Table table) throws IOException {
        TreeSet<Long> liveIds = new TreeSet<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                liveIds.add((Long) record.getField("id"));
            }
        }
        return liveIds;
    }

    private VendoredFixtureVerifier() {}
}
