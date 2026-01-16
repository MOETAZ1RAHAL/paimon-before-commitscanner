/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.operation.commit;

import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.List;

/** Changes to commit. */
public class CommitChanges {

    private final List<ManifestEntry> tableFiles;
    private final List<ManifestEntry> changelogFiles;
    private final List<IndexManifestEntry> indexFiles;

    public CommitChanges(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles) {
        this.tableFiles = tableFiles;
        this.changelogFiles = changelogFiles;
        this.indexFiles = indexFiles;
    }

    public List<ManifestEntry> tableFiles() {
        return tableFiles;
    }

    public List<ManifestEntry> changelogFiles() {
        return changelogFiles;
    }

    public List<IndexManifestEntry> indexFiles() {
        return indexFiles;
    }
}
