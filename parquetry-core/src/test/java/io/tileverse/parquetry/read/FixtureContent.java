/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

/**
 * Single source of truth for the V1 fixture corpus: schemas, row counts, and the deterministic content of every row.
 * Both {@link V1FixtureGenerator} (writer side) and {@code DataPageV1ConformanceTest} (reader side) consult this class
 * so the test can recompute the expected payload from the row index without re-reading the file with a second oracle.
 *
 * <p>Row counts are large enough to keep dictionary encoding meaningful (many duplicates / small distinct set) but
 * small enough that every fixture stays well under 10 KB on disk.
 */
public final class FixtureContent {

    private FixtureContent() {}

    // --- file names ---

    public static final String PLAIN_INT_STRING = "plain-int-string.parquet";
    public static final String DICT_STRING = "dict-string.parquet";
    public static final String GZIP_INT_STRING = "gzip-int-string.parquet";
    public static final String SNAPPY_INT_STRING = "snappy-int-string.parquet";
    public static final String NULLABLE_STRING = "nullable-string.parquet";

    // --- row counts ---

    public static final int PLAIN_ROW_COUNT = 60;
    public static final int DICT_ROW_COUNT = 200;
    public static final int GZIP_ROW_COUNT = 200;
    public static final int SNAPPY_ROW_COUNT = 200;
    public static final int NULLABLE_ROW_COUNT = 80;

    // --- schemas ---

    public static final String SCHEMA_YEAR_COUNTRY = """
            {"type":"record","name":"YearCountry","fields":[
              {"name":"year","type":"int"},
              {"name":"country","type":"string"}
            ]}""";

    static final String SCHEMA_YEAR_REGION = """
            {"type":"record","name":"YearRegion","fields":[
              {"name":"year","type":"int"},
              {"name":"region","type":"string"}
            ]}""";

    static final String SCHEMA_ID_OPTIONAL_LABEL = """
            {"type":"record","name":"IdLabel","fields":[
              {"name":"id","type":"int"},
              {"name":"optional_label","type":["null","string"],"default":null}
            ]}""";

    // --- row content ---

    private static final String[] REGIONS = {"north", "south", "east", "west"};

    static int year(int rowIndex) {
        return 2020 + (rowIndex % 5);
    }

    /** Per-row distinct strings - keeps the {@code plain-int-string} fixture out of the dictionary-encoding path. */
    static String distinctCountry(int rowIndex) {
        return "country-" + rowIndex;
    }

    /** Small alphabet of values - drives dictionary encoding in every fixture that uses this column. */
    static String region(int rowIndex) {
        return REGIONS[rowIndex % REGIONS.length];
    }

    /** Even-indexed rows are null; odd-indexed rows are a deterministic non-null label. */
    static String optionalLabel(int rowIndex) {
        return (rowIndex % 2 == 0) ? null : "label-" + rowIndex;
    }
}
