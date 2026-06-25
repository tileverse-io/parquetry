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
package io.tileverse.parquetry.dataset;

import java.util.List;

/**
 * The ordered, pruned set of files a {@link ParquetDataset} read will visit, each a {@link PlannedFile}; exposed for
 * diagnostics (which files survived pruning). This is distinct from {@link FilesetReader}, the low-level index-based
 * byte-source adapter consumed by {@link ParquetSource#open(FilesetReader)}.
 */
public interface FilePlan {

    List<PlannedFile> files();
}
