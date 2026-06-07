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
 * The machine resource facts the read path sizes its native and disk budgets from, and the caps derived from them.
 *
 * <p>{@link io.tileverse.parquetry.io.limits.ResourceLimits} is an injectable port reporting the raw facts - permitted
 * off-heap memory, usable disk, processor count, and the spill directory. The default reads the running container; a
 * test supplies a fixed instance for a deterministic bound independent of the host.
 *
 * <p>{@link io.tileverse.parquetry.io.limits.IoLimits} derives the off-heap and spill caps by applying conservative
 * fractions to those facts. This is the single home for that sizing policy; the budgets in the core read path consume
 * the caps and own no sizing of their own.
 */
package io.tileverse.parquetry.io.limits;
