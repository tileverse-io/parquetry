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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class WriteObserverTest {

    @Test
    void noneIsANoOp() {
        WriteObserver none = WriteObserver.NONE;

        assertThatNoException().isThrownBy(() -> {
            none.onRowsWritten(100);
            none.onWriteFinished(new WriteStats(100, 1, 10, 20, Duration.ZERO));
        });
    }

    @Test
    void compositeFansOutEachCallbackToEveryObserver() {
        List<String> calls = new ArrayList<>();
        WriteObserver one = new WriteObserver() {
            @Override
            public void onRowsWritten(long totalRows) {
                calls.add("one:" + totalRows);
            }

            @Override
            public void onWriteFinished(WriteStats stats) {
                calls.add("one:done");
            }
        };
        WriteObserver two = new WriteObserver() {
            @Override
            public void onRowsWritten(long totalRows) {
                calls.add("two:" + totalRows);
            }
        };

        WriteObserver composite = WriteObserver.composite(one, two);
        composite.onRowsWritten(50);
        composite.onWriteFinished(new WriteStats(50, 1, 5, 10, Duration.ZERO));

        assertThat(calls).containsExactly("one:50", "two:50", "one:done");
    }
}
