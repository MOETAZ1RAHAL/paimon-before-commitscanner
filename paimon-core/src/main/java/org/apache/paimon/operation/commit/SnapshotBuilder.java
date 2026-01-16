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

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;
import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;

/** Builder for {@link Snapshot}. */
public class SnapshotBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotBuilder.class);

    private final SchemaManager schemaManager;
    private final String tableName;
    private final String commitUser;
    private final SnapshotManager snapshotManager;
    private final StatsFileHandler statsFileHandler;

    public SnapshotBuilder(
            SchemaManager schemaManager,
            String tableName,
            String commitUser,
            SnapshotManager snapshotManager,
            StatsFileHandler statsFileHandler) {
        this.schemaManager = schemaManager;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.statsFileHandler = statsFileHandler;
    }

    public Snapshot build(
            @Nullable Snapshot latestSnapshot,
            long identifier,
            CommitKind commitKind,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties,
            @Nullable String newStatsFileName,
            long newSnapshotId,
            ManifestAccumulator.Result manifestResult,
            List<ManifestEntry> changelogFiles) {
        long latestSchemaId =
                schemaManager.latestOrThrow("Cannot get latest schema for table " + tableName).id();

        // write new stats or inherit from the previous snapshot
        String statsFileName = null;
        if (newStatsFileName != null) {
            statsFileName = newStatsFileName;
        } else if (latestSnapshot != null) {
            Optional<Statistics> previousStatistic = statsFileHandler.readStats(latestSnapshot);
            if (previousStatistic.isPresent()) {
                if (previousStatistic.get().schemaId() != latestSchemaId) {
                    LOG.warn("Schema changed, stats will not be inherited");
                } else {
                    statsFileName = latestSnapshot.statistics();
                }
            }
        }

        // the added records subtract the deleted records from
        long deltaRecordCount =
                recordCountAdd(manifestResult.deltaFiles())
                        - recordCountDelete(manifestResult.deltaFiles());
        long totalRecordCount = manifestResult.previousTotalRecordCount() + deltaRecordCount;

        return new Snapshot(
                newSnapshotId,
                latestSchemaId,
                manifestResult.baseManifestList().getLeft(),
                manifestResult.baseManifestList().getRight(),
                manifestResult.deltaManifestList().getLeft(),
                manifestResult.deltaManifestList().getRight(),
                manifestResult.changelogManifestList() == null
                        ? null
                        : manifestResult.changelogManifestList().getLeft(),
                manifestResult.changelogManifestList() == null
                        ? null
                        : manifestResult.changelogManifestList().getRight(),
                manifestResult.indexManifest(),
                commitUser,
                identifier,
                commitKind,
                System.currentTimeMillis(),
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                recordCount(changelogFiles),
                manifestResult.currentWatermark(),
                statsFileName,
                // if empty properties, just set to null
                properties.isEmpty() ? null : properties,
                manifestResult.nextRowIdStart());
    }
}
