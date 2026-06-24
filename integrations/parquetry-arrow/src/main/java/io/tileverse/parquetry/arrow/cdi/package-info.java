/*
 * Copyright (c) 2026 Multivers.io
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

/**
 * Exports parquetry read output over the Arrow C Data Interface: a hand-rolled, zero-copy {@code ArrowArrayStream} a
 * native consumer (DuckDB, Polars, any Arrow C consumer) pulls batches from. The structs are modeled as Java FFM
 * {@link java.lang.foreign.MemoryLayout} mirrors of the C ABI, the callbacks as {@link java.lang.foreign.Linker} upcall
 * stubs, and each exported array's buffers are borrowed from a segment pool and returned on the array's release.
 *
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C Data Interface</a>
 */
package io.tileverse.parquetry.arrow.cdi;
