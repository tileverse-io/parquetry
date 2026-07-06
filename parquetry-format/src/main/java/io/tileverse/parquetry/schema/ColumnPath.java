/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Dot-separated path to a column within a Parquet schema tree.
 *
 * <p>The root group is not part of the path; the first segment is a direct child of root. A flat column "year" has the
 * single-segment path {@code ColumnPath.of("year")}; a leaf "xmin" nested in a group "bbox" has
 * {@code ColumnPath.of("bbox", "xmin")}.
 *
 * <p>The path stores its dot-joined form, which makes equality and hashing plain {@link String} operations - the
 * dominant cost when a path is used as a map key while reading a file. Segment access ({@link #numParts()},
 * {@link #part(int)}, {@link #name()}) reads from that string and never materializes a list.
 *
 * <p>The factories ({@link #of(String...)}, {@link #of(List)}, {@link #parse(String)}) validate their input. The
 * canonical constructor trusts an already-validated dot-joined form; because it runs on every read-path construction,
 * it does no scanning work.
 *
 * @param dot the dot-joined path, e.g. {@code "bbox.xmin"}; never blank and never with an empty segment
 */
public record ColumnPath(String dot) {

    private static final char SEPARATOR = '.';

    public ColumnPath {
        Objects.requireNonNull(dot, "dot");
    }

    /** Creates a path from individual name segments; each segment must be non-blank and contain no dot. */
    public static ColumnPath of(String... segments) {
        if (segments.length == 1) {
            return ofSingle(segments[0]);
        }
        return of(Arrays.asList(segments));
    }

    /**
     * Creates a path from an ordered list of name segments, such as a Thrift {@code path_in_schema} or a GeoParquet
     * covering path. Each segment must be non-blank and contain no dot. The list is read only during construction; the
     * resulting path holds no list.
     */
    public static ColumnPath of(List<String> segments) {
        int count = segments.size();
        if (count == 0) {
            throw new IllegalArgumentException("A column path needs at least one segment");
        }
        if (count == 1) {
            return ofSingle(segments.get(0));
        }
        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < count; i++) {
            String segment = segments.get(i);
            requireValidSegment(segment);
            if (i > 0) {
                joined.append(SEPARATOR);
            }
            joined.append(segment);
        }
        return new ColumnPath(joined.toString());
    }

    /** Parses an already dot-joined path, rejecting an empty, leading, trailing, or doubled separator. */
    public static ColumnPath parse(String dotted) {
        requireValidDotForm(dotted);
        return new ColumnPath(dotted);
    }

    /** The number of name segments in this path; always at least one. */
    public int numParts() {
        int count = 1;
        for (int i = 0; i < dot.length(); i++) {
            if (dot.charAt(i) == SEPARATOR) {
                count++;
            }
        }
        return count;
    }

    /** The name segment at {@code index}, counting from the root child at index 0. */
    public String part(int index) {
        int start = 0;
        int seen = 0;
        for (int i = 0; i <= dot.length(); i++) {
            if (i == dot.length() || dot.charAt(i) == SEPARATOR) {
                if (seen == index) {
                    return dot.substring(start, i);
                }
                seen++;
                start = i + 1;
            }
        }
        throw new IndexOutOfBoundsException("Part index " + index + " out of range for path " + dot);
    }

    /** The leaf segment, i.e. the last name in the path. */
    public String name() {
        int lastSeparator = dot.lastIndexOf(SEPARATOR);
        return lastSeparator < 0 ? dot : dot.substring(lastSeparator + 1);
    }

    private static ColumnPath ofSingle(String segment) {
        requireValidSegment(segment);
        return new ColumnPath(segment);
    }

    private static void requireValidDotForm(String dot) {
        if (dot == null || dot.isEmpty()) {
            throw new IllegalArgumentException("A column path must not be blank");
        }
        int segmentStart = 0;
        for (int i = 0; i <= dot.length(); i++) {
            boolean atBoundary = i == dot.length() || dot.charAt(i) == SEPARATOR;
            if (atBoundary) {
                if (i == segmentStart) {
                    throw new IllegalArgumentException("Column path has an empty segment: '" + dot + "'");
                }
                segmentStart = i + 1;
            }
        }
    }

    private static void requireValidSegment(String segment) {
        if (segment == null || segment.isBlank()) {
            throw new IllegalArgumentException("A column path segment must not be blank");
        }
        if (segment.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException("A column path segment must not contain '.': '" + segment + "'");
        }
    }
}
