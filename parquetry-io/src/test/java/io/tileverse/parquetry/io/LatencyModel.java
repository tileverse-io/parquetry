/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.io;

/**
 * Analytic wall-clock projection for a recorded read: one round trip per request plus payload transfer at the given
 * bandwidth. No sleeping; CI assertions project several round-trip/bandwidth points from one recorded run. The serial
 * model is faithful per row group (a row group's coalesced ranges are issued serially); under cross-row-group prefetch
 * parallelism it is an upper bound.
 */
public final class LatencyModel {

    private LatencyModel() {}

    /**
     * @param requestCount number of individual read requests issued
     * @param bytesRead total payload bytes transferred
     * @param rttNanos round-trip time of one request, in nanoseconds
     * @param bytesPerSecond available bandwidth, in bytes per second; must be positive
     * @return the projected wall-clock time, in nanoseconds
     */
    public static long wallNanos(long requestCount, long bytesRead, long rttNanos, long bytesPerSecond) {
        if (bytesPerSecond <= 0) {
            throw new IllegalArgumentException("bytesPerSecond must be positive, got " + bytesPerSecond);
        }
        double transferNanos = bytesRead * (1_000_000_000.0 / bytesPerSecond);
        return requestCount * rttNanos + Math.round(transferNanos);
    }
}
