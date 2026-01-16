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
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Accumulator for manifests. */
public class ManifestAccumulator {

    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    private final long manifestTargetSize;
    private final long manifestFullCompactionSize;
    private final int manifestMergeMinCount;
    private final RowType partitionType;
    @Nullable private final Integer manifestReadParallelism;
    private final BucketMode bucketMode;
    private final boolean rowTrackingEnabled;
    private final FileStoreScan scan;

    public ManifestAccumulator(
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexManifestFile indexManifestFile,
            long manifestTargetSize,
            long manifestFullCompactionSize,
            int manifestMergeMinCount,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism,
            BucketMode bucketMode,
            boolean rowTrackingEnabled,
            FileStoreScan scan) {
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.indexManifestFile = indexManifestFile;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.partitionType = partitionType;
        this.manifestReadParallelism = manifestReadParallelism;
        this.bucketMode = bucketMode;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.scan = scan;
    }

    public Result accumulate(
            @Nullable Snapshot latestSnapshot,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            long newSnapshotId,
            long firstRowIdStart) {
        Pair<String, Long> baseManifestList = null;
        Pair<String, Long> deltaManifestList = null;
        List<PartitionEntry> deltaStatistics;
        Pair<String, Long> changelogManifestList = null;
        String oldIndexManifest = null;
        String indexManifest = null;
        List<ManifestFileMeta> mergeBeforeManifests = new ArrayList<>();
        List<ManifestFileMeta> mergeAfterManifests = new ArrayList<>();
        long previousTotalRecordCount = 0L;
        Long currentWatermark = watermark;
        long nextRowIdStart = firstRowIdStart;

        try {
            if (latestSnapshot != null) {
                previousTotalRecordCount = scan.totalRecordCount(latestSnapshot);
                // read all previous manifest files
                mergeBeforeManifests = manifestList.readDataManifests(latestSnapshot);
                // read the last snapshot to complete the bucket's logOffsets when logOffsets does
                // not
                // contain all buckets
                Map<Integer, Long> latestLogOffsets = latestSnapshot.logOffsets();
                if (latestLogOffsets != null) {
                    latestLogOffsets.forEach(logOffsets::putIfAbsent);
                }
                Long latestWatermark = latestSnapshot.watermark();
                if (latestWatermark != null) {
                    currentWatermark =
                            currentWatermark == null
                                    ? latestWatermark
                                    : Math.max(currentWatermark, latestWatermark);
                }
                oldIndexManifest = latestSnapshot.indexManifest();
            }

            // try to merge old manifest files to create base manifest list
            mergeAfterManifests =
                    ManifestFileMerger.merge(
                            mergeBeforeManifests,
                            manifestFile,
                            manifestTargetSize,
                            manifestMergeMinCount,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            baseManifestList = manifestList.write(mergeAfterManifests);

            if (rowTrackingEnabled) {
                // assigned snapshot id to delta files
                List<ManifestEntry> snapshotAssigned = new ArrayList<>();
                assignSnapshotId(newSnapshotId, deltaFiles, snapshotAssigned);
                // assign row id for new files
                List<ManifestEntry> rowIdAssigned = new ArrayList<>();
                nextRowIdStart =
                        assignRowTrackingMeta(firstRowIdStart, snapshotAssigned, rowIdAssigned);
                deltaFiles = rowIdAssigned;
            }

            // the added records subtract the deleted records from
            deltaStatistics = new ArrayList<>(PartitionEntry.merge(deltaFiles));
            deltaManifestList = manifestList.write(manifestFile.write(deltaFiles));

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogManifestList = manifestList.write(manifestFile.write(changelogFiles));
            }

            indexManifest =
                    indexManifestFile.writeIndexFiles(oldIndexManifest, indexFiles, bucketMode);

            return new Result(
                    baseManifestList,
                    deltaManifestList,
                    changelogManifestList,
                    oldIndexManifest,
                    indexManifest,
                    mergeBeforeManifests,
                    mergeAfterManifests,
                    deltaStatistics,
                    deltaFiles,
                    previousTotalRecordCount,
                    currentWatermark,
                    nextRowIdStart);
        } catch (Throwable e) {
            cleanUpReuseTmpManifests(
                    deltaManifestList, changelogManifestList, oldIndexManifest, indexManifest);
            cleanUpNoReuseTmpManifests(baseManifestList, mergeBeforeManifests, mergeAfterManifests);
            throw e;
        }
    }

    private long assignRowTrackingMeta(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> rowIdAssigned) {
        if (deltaFiles.isEmpty()) {
            return firstRowIdStart;
        }
        // assign row id for new files
        long start = firstRowIdStart;
        long blobStart = firstRowIdStart;
        for (ManifestEntry entry : deltaFiles) {
            checkArgument(
                    entry.file().fileSource().isPresent(),
                    "This is a bug, file source field for row-tracking table must present.");
            if (entry.file().fileSource().get().equals(FileSource.APPEND)
                    && entry.file().firstRowId() == null) {
                if (isBlobFile(entry.file().fileName())) {
                    if (blobStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, blobStart %d should be less than start %d when assigning a blob entry file.",
                                        blobStart, start));
                    }
                    long rowCount = entry.file().rowCount();
                    rowIdAssigned.add(entry.assignFirstRowId(blobStart));
                    blobStart += rowCount;
                } else {
                    long rowCount = entry.file().rowCount();
                    rowIdAssigned.add(entry.assignFirstRowId(start));
                    blobStart = start;
                    start += rowCount;
                }
            } else {
                // for compact file, do not assign first row id.
                rowIdAssigned.add(entry);
            }
        }
        return start;
    }

    private void assignSnapshotId(
            long snapshotId, List<ManifestEntry> deltaFiles, List<ManifestEntry> snapshotAssigned) {
        for (ManifestEntry entry : deltaFiles) {
            snapshotAssigned.add(entry.assignSequenceNumber(snapshotId, snapshotId));
        }
    }

    public void cleanUpNoReuseTmpManifests(
            Pair<String, Long> baseManifestList,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests) {
        if (baseManifestList != null) {
            manifestList.delete(baseManifestList.getKey());
        }
        Set<String> oldMetaSet =
                mergeBeforeManifests.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (ManifestFileMeta suspect : mergeAfterManifests) {
            if (!oldMetaSet.contains(suspect.fileName())) {
                manifestFile.delete(suspect.fileName());
            }
        }
    }

    public void cleanUpReuseTmpManifests(
            Pair<String, Long> deltaManifestList,
            Pair<String, Long> changelogManifestList,
            String oldIndexManifest,
            String newIndexManifest) {
        if (deltaManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(deltaManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(deltaManifestList.getKey());
        }

        if (changelogManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(changelogManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(changelogManifestList.getKey());
        }

        cleanIndexManifest(oldIndexManifest, newIndexManifest);
    }

    public void cleanIndexManifest(String oldIndexManifest, String newIndexManifest) {
        if (newIndexManifest != null && !Objects.equals(oldIndexManifest, newIndexManifest)) {
            indexManifestFile.delete(newIndexManifest);
        }
    }

    /** Result of {@link ManifestAccumulator#accumulate}. */
    public static class Result {
        private final Pair<String, Long> baseManifestList;
        private final Pair<String, Long> deltaManifestList;
        private final Pair<String, Long> changelogManifestList;
        private final String oldIndexManifest;
        private final String indexManifest;
        private final List<ManifestFileMeta> mergeBeforeManifests;
        private final List<ManifestFileMeta> mergeAfterManifests;
        private final List<PartitionEntry> deltaStatistics;
        private final List<ManifestEntry> deltaFiles;
        private final long previousTotalRecordCount;
        private final Long currentWatermark;
        private final long nextRowIdStart;

        public Result(
                Pair<String, Long> baseManifestList,
                Pair<String, Long> deltaManifestList,
                Pair<String, Long> changelogManifestList,
                String oldIndexManifest,
                String indexManifest,
                List<ManifestFileMeta> mergeBeforeManifests,
                List<ManifestFileMeta> mergeAfterManifests,
                List<PartitionEntry> deltaStatistics,
                List<ManifestEntry> deltaFiles,
                long previousTotalRecordCount,
                Long currentWatermark,
                long nextRowIdStart) {
            this.baseManifestList = baseManifestList;
            this.deltaManifestList = deltaManifestList;
            this.changelogManifestList = changelogManifestList;
            this.oldIndexManifest = oldIndexManifest;
            this.indexManifest = indexManifest;
            this.mergeBeforeManifests = mergeBeforeManifests;
            this.mergeAfterManifests = mergeAfterManifests;
            this.deltaStatistics = deltaStatistics;
            this.deltaFiles = deltaFiles;
            this.previousTotalRecordCount = previousTotalRecordCount;
            this.currentWatermark = currentWatermark;
            this.nextRowIdStart = nextRowIdStart;
        }

        public Pair<String, Long> baseManifestList() {
            return baseManifestList;
        }

        public Pair<String, Long> deltaManifestList() {
            return deltaManifestList;
        }

        public Pair<String, Long> changelogManifestList() {
            return changelogManifestList;
        }

        public String oldIndexManifest() {
            return oldIndexManifest;
        }

        public String indexManifest() {
            return indexManifest;
        }

        public List<ManifestFileMeta> mergeBeforeManifests() {
            return mergeBeforeManifests;
        }

        public List<ManifestFileMeta> mergeAfterManifests() {
            return mergeAfterManifests;
        }

        public List<PartitionEntry> deltaStatistics() {
            return deltaStatistics;
        }

        public List<ManifestEntry> deltaFiles() {
            return deltaFiles;
        }

        public long previousTotalRecordCount() {
            return previousTotalRecordCount;
        }

        public Long currentWatermark() {
            return currentWatermark;
        }

        public long nextRowIdStart() {
            return nextRowIdStart;
        }
    }
}
