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
package io.tileverse.parquetry.internal.read;

/** How a column reader materializes a data page's values. */
enum ValueDecode {

    /** Decodes every value as the page loads; each batch slices the decoded page. */
    EAGER,

    /** Decodes the whole page's surviving rows as it loads, skipping the values of the rows the row mask drops. */
    PAGE_MASK,

    /**
     * Loads a page's levels and validity without its values and keeps the page's decoder open across the page's
     * windows; each window decodes only the value slots its surviving rows cover.
     */
    WINDOWED_MASK
}
