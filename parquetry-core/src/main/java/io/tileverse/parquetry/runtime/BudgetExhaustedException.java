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
package io.tileverse.parquetry.runtime;

/**
 * Thrown when a buffer needed by the read to make progress could not be placed anywhere: the memory budget had no room
 * within the wait allowed for it, and the disk budget is exhausted as well. The process is overloaded with reads beyond
 * what its budgets were sized for; a server turns this into one failed request rather than a stalled one.
 */
public class BudgetExhaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BudgetExhaustedException(String message) {
        super(message);
    }
}
