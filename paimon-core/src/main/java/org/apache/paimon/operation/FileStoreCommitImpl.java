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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitMessageCollector;
import org.apache.paimon.operation.commit.CommitResult;
import org.apache.paimon.operation.commit.CommitResult.RetryResult;
import org.apache.paimon.operation.commit.CommitResult.SuccessResult;
import org.apache.paimon.operation.commit.CommitScanner;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.operation.commit.ConflictDetection.ConflictCheck;
import org.apache.paimon.operation.commit.ManifestAccumulator;
import org.apache.paimon.operation.commit.SnapshotBuilder;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.commit.ConflictDetection.hasConflictChecked;
import static org.apache.paimon.operation.commit.ConflictDetection.mustConflictCheck;
import static org.apache.paimon.operation.commit.ConflictDetection.noConflictCheck;
import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link FileStoreCommit}.
 *
 * <p>This class provides an atomic commit method to the user.
 *
 * <ol>
 *   <li>Before calling {@link #commit}, if user cannot determine if this commit is done before,
 *       user should first call {@link #filterCommitted}.
 *   <li>Before committing, it will first check for conflicts by checking if all files to be removed
 *       currently exists, and if modified files have overlapping key ranges with existing files.
 *   <li>After that it use the external {@link SnapshotCommit} (if provided) or the atomic rename of
 *       the file system to ensure atomicity.
 *   <li>If commit fails due to conflicts or exception it tries its best to clean up and aborts.
 *   <li>If atomic rename fails it tries again after reading the latest snapshot from step 2.
 * </ol>
 *
 * <p>NOTE: If you want to modify this class, any exception during commit MUST NOT BE IGNORED. They
 * must be thrown to restart the job. It is recommended to run FileStoreCommitTest thousands of
 * times to make sure that your changes are correct.
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final SnapshotCommit snapshotCommit;
    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final String tableName;
    private final String commitUser;
    private final RowType partitionType;
    private final CoreOptions options;
    private final String partitionDefaultName;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    private final FileStoreScan scan;
    private final int numBucket;
    private final MemorySize manifestTargetSize;
    private final MemorySize manifestFullCompactionSize;
    private final int manifestMergeMinCount;
    private final boolean dynamicPartitionOverwrite;
    private final String branchName;
    @Nullable private final Integer manifestReadParallelism;
    private final List<CommitCallback> commitCallbacks;
    private final StatsFileHandler statsFileHandler;
    private final BucketMode bucketMode;
    private final long commitTimeout;
    private final long commitMinRetryWait;
    private final long commitMaxRetryWait;
    private final int commitMaxRetries;
    @Nullable private Long strictModeLastSafeSnapshot;
    private final InternalRowPartitionComputer partitionComputer;
    private final boolean rowTrackingEnabled;
    private final boolean discardDuplicateFiles;
    private final ConflictDetection conflictDetection;

    private final CommitMessageCollector commitMessageCollector;
    private final CommitScanner commitScanner;
    private final ManifestAccumulator manifestAccumulator;
    private final SnapshotBuilder snapshotBuilder;

    private boolean ignoreEmptyCommit;
    private CommitMetrics commitMetrics;
    private boolean appendCommitCheckConflict = false;

    public FileStoreCommitImpl(
            SnapshotCommit snapshotCommit,
            FileIO fileIO,
            SchemaManager schemaManager,
            String tableName,
            String commitUser,
            RowType partitionType,
            CoreOptions options,
            String partitionDefaultName,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            IndexManifestFile.Factory indexManifestFileFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            MemorySize manifestFullCompactionSize,
            int manifestMergeMinCount,
            boolean dynamicPartitionOverwrite,
            String branchName,
            StatsFileHandler statsFileHandler,
            BucketMode bucketMode,
            @Nullable Integer manifestReadParallelism,
            List<CommitCallback> commitCallbacks,
            int commitMaxRetries,
            long commitTimeout,
            long commitMinRetryWait,
            long commitMaxRetryWait,
            @Nullable Long strictModeLastSafeSnapshot,
            boolean rowTrackingEnabled,
            boolean discardDuplicateFiles,
            ConflictDetection conflictDetection) {
        this.snapshotCommit = snapshotCommit;
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.options = options;
        this.partitionDefaultName = partitionDefaultName;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.scan = scan;
        // Stats in DELETE Manifest Entries is useless
        if (options.manifestDeleteFileDropStats()) {
            this.scan.dropStats();
        }
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.branchName = branchName;
        this.manifestReadParallelism = manifestReadParallelism;
        this.commitCallbacks = commitCallbacks;
        this.commitMaxRetries = commitMaxRetries;
        this.commitTimeout = commitTimeout;
        this.commitMinRetryWait = commitMinRetryWait;
        this.commitMaxRetryWait = commitMaxRetryWait;
        this.strictModeLastSafeSnapshot = strictModeLastSafeSnapshot;
        this.partitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        partitionType,
                        partitionType.getFieldNames().toArray(new String[0]),
                        options.legacyPartitionName());

        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
        this.bucketMode = bucketMode;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.discardDuplicateFiles = discardDuplicateFiles;
        this.conflictDetection = conflictDetection;

        this.commitMessageCollector =
                new CommitMessageCollector(numBucket, options, conflictDetection);
        this.commitScanner = new CommitScanner(scan);
        this.manifestAccumulator =
                new ManifestAccumulator(
                        this.manifestFile,
                        this.manifestList,
                        this.indexManifestFile,
                        manifestTargetSize.getBytes(),
                        manifestFullCompactionSize.getBytes(),
                        manifestMergeMinCount,
                        partitionType,
                        manifestReadParallelism,
                        bucketMode,
                        rowTrackingEnabled,
                        scan);
        this.snapshotBuilder =
                new SnapshotBuilder(
                        schemaManager, tableName, commitUser, snapshotManager, statsFileHandler);
    }

    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    @Override
    public FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire) {
        this.conflictDetection.withPartitionExpire(partitionExpire);
        return this;
    }

    @Override
    public FileStoreCommit appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }

    @Override
    public List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables) {
        // nothing to filter, fast exit
        if (committables.isEmpty()) {
            return committables;
        }

        for (int i = 1; i < committables.size(); i++) {
            checkArgument(
                    committables.get(i).identifier() > committables.get(i - 1).identifier(),
                    "Committables must be sorted according to identifiers before filtering. This is unexpected.");
        }

        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            List<ManifestCommittable> result = new ArrayList<>();
            for (ManifestCommittable committable : committables) {
                // if committable is newer than latest snapshot, then it hasn't been committed
                if (committable.identifier() > latestSnapshot.get().commitIdentifier()) {
                    result.add(committable);
                } else {
                    commitCallbacks.forEach(callback -> callback.retry(committable));
                }
            }
            return result;
        } else {
            // if there is no previous snapshots then nothing should be filtered
            return committables;
        }
    }

    @Override
    public int commit(ManifestCommittable committable, boolean checkAppendFiles) {
        LOG.info(
                "Ready to commit to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n{}", committable);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        Snapshot latestSnapshot = null;
        Long safeLatestSnapshotId = null;
        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendIndexFiles = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> compactIndexFiles = new ArrayList<>();
        commitMessageCollector.collect(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                appendIndexFiles,
                compactTableFiles,
                compactChangelog,
                compactIndexFiles);
        try {
            List<SimpleFileEntry> appendSimpleEntries = SimpleFileEntry.from(appendTableFiles);
            if (!ignoreEmptyCommit
                    || !appendTableFiles.isEmpty()
                    || !appendChangelog.isEmpty()
                    || !appendIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 1:
                // Read manifest entries from changed partitions here and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                latestSnapshot = snapshotManager.latestSnapshot();
                CommitKind commitKind = CommitKind.APPEND;
                ConflictCheck conflictCheck = noConflictCheck();
                if (containsFileDeletionOrDeletionVectors(appendSimpleEntries, appendIndexFiles)) {
                    commitKind = CommitKind.OVERWRITE;
                    conflictCheck = mustConflictCheck();
                } else if (latestSnapshot != null && appendCommitCheckConflict) {
                    conflictCheck = mustConflictCheck();
                }

                boolean discardDuplicate = discardDuplicateFiles && commitKind == CommitKind.APPEND;
                if (discardDuplicate) {
                    checkAppendFiles = true;
                }

                if (latestSnapshot != null && checkAppendFiles) {
                    // it is possible that some partitions only have compact changes,
                    // so we need to contain all changes
                    baseEntries.addAll(
                            commitScanner.readAllEntriesFromChangedPartitions(
                                    latestSnapshot,
                                    commitScanner.changedPartitions(
                                            appendTableFiles,
                                            compactTableFiles,
                                            appendIndexFiles)));
                    if (discardDuplicate) {
                        Set<FileEntry.Identifier> baseIdentifiers =
                                baseEntries.stream()
                                        .map(FileEntry::identifier)
                                        .collect(Collectors.toSet());
                        appendTableFiles =
                                appendTableFiles.stream()
                                        .filter(
                                                entry ->
                                                        !baseIdentifiers.contains(
                                                                entry.identifier()))
                                        .collect(Collectors.toList());
                        appendSimpleEntries = SimpleFileEntry.from(appendTableFiles);
                    }
                    conflictDetection.checkNoConflictsOrFail(
                            latestSnapshot,
                            baseEntries,
                            appendSimpleEntries,
                            appendIndexFiles,
                            commitKind);
                    safeLatestSnapshotId = latestSnapshot.id();
                }

                attempts +=
                        tryCommit(
                                provider(appendTableFiles, appendChangelog, appendIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                committable.properties(),
                                provider(commitKind),
                                conflictCheck,
                                null);
                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty()
                    || !compactChangelog.isEmpty()
                    || !compactIndexFiles.isEmpty()) {
                // Optimization for common path.
                // Step 2:
                // Add appendChanges to the manifest entries read above and check for conflicts.
                // If there are no other jobs committing at the same time,
                // we can skip conflict checking in tryCommit method.
                // This optimization is mainly used to decrease the number of times we read from
                // files.
                if (safeLatestSnapshotId != null) {
                    baseEntries.addAll(appendSimpleEntries);
                    conflictDetection.checkNoConflictsOrFail(
                            latestSnapshot,
                            baseEntries,
                            SimpleFileEntry.from(compactTableFiles),
                            compactIndexFiles,
                            CommitKind.COMPACT);
                    // assume this compact commit follows just after the append commit created above
                    safeLatestSnapshotId += 1;
                }

                attempts +=
                        tryCommit(
                                provider(compactTableFiles, compactChangelog, compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                committable.properties(),
                                provider(CommitKind.COMPACT),
                                hasConflictChecked(safeLatestSnapshotId),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info(
                    "Finished (Uncertain of success) commit to table {}, duration {} ms",
                    tableName,
                    commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        appendChangelog,
                        compactTableFiles,
                        compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
    }

    private void reportCommit(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        commitDuration,
                        generatedSnapshots,
                        attempts);
        commitMetrics.reportCommit(commitStats);
    }

    private <T extends FileEntry> boolean containsFileDeletionOrDeletionVectors(
            List<T> appendFileEntries, List<IndexManifestEntry> appendIndexFiles) {
        for (T appendFileEntry : appendFileEntries) {
            if (appendFileEntry.kind().equals(FileKind.DELETE)) {
                return true;
            }
        }
        for (IndexManifestEntry appendIndexFile : appendIndexFiles) {
            if (appendIndexFile.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int overwritePartition(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties) {
        LOG.info(
                "Ready to overwrite to table {}, number of commit messages: {}",
                tableName,
                committable.fileCommittables().size());
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to overwrite partition {}\nManifestCommittable: {}\nProperties: {}",
                    partition,
                    committable,
                    properties);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendIndexFiles = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> compactIndexFiles = new ArrayList<>();
        commitMessageCollector.collect(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                appendIndexFiles,
                compactTableFiles,
                compactChangelog,
                compactIndexFiles);

        if (!appendChangelog.isEmpty() || !compactChangelog.isEmpty()) {
            StringBuilder warnMessage =
                    new StringBuilder(
                            "Overwrite mode currently does not commit any changelog.\n"
                                    + "Please make sure that the partition you're overwriting "
                                    + "is not being consumed by a streaming reader.\n"
                                    + "Ignored changelog files are:\n");
            for (ManifestEntry entry : appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString());
        }

        try {
            boolean skipOverwrite = false;
            // partition filter is built from static or dynamic partition according to properties
            PartitionPredicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                if (appendTableFiles.isEmpty()) {
                    // in dynamic mode, if there is no changes to commit, no data will be deleted
                    skipOverwrite = true;
                } else {
                    Set<BinaryRow> partitions =
                            appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .collect(Collectors.toSet());
                    partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
                }
            } else {
                // partition may be partial partition fields, so here must use predicate way.
                Predicate partitionPredicate =
                        createPartitionPredicate(partition, partitionType, partitionDefaultName);
                partitionFilter =
                        PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
                // sanity check, all changes must be done within the given partition
                if (partitionFilter != null) {
                    for (ManifestEntry entry : appendTableFiles) {
                        if (!partitionFilter.test(entry.partition())) {
                            throw new IllegalArgumentException(
                                    "Trying to overwrite partition "
                                            + partition
                                            + ", but the changes in "
                                            + pathFactory.getPartitionString(entry.partition())
                                            + " does not belong to this partition");
                        }
                    }
                }
            }

            boolean withCompact = !compactTableFiles.isEmpty() || !compactIndexFiles.isEmpty();

            if (!withCompact) {
                // try upgrade
                appendTableFiles = commitMessageCollector.tryUpgrade(appendTableFiles);
            }

            // overwrite new files
            if (!skipOverwrite) {
                attempts +=
                        tryOverwritePartition(
                                partitionFilter,
                                appendTableFiles,
                                appendIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                committable.properties());
                generatedSnapshot += 1;
            }

            if (withCompact) {
                attempts +=
                        tryCommit(
                                provider(compactTableFiles, emptyList(), compactIndexFiles),
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                committable.properties(),
                                provider(CommitKind.COMPACT),
                                mustConflictCheck(),
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            LOG.info("Finished overwrite to table {}, duration {} ms", tableName, commitDuration);
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        emptyList(),
                        compactTableFiles,
                        emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
        return generatedSnapshot;
    }

    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        checkArgument(!partitions.isEmpty(), "Partitions list cannot be empty.");

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Ready to drop partitions {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        boolean fullMode =
                partitions.stream().allMatch(part -> part.size() == partitionType.getFieldCount());
        PartitionPredicate partitionFilter;
        if (fullMode) {
            List<BinaryRow> binaryPartitions =
                    createBinaryPartitions(partitions, partitionType, partitionDefaultName);
            partitionFilter = PartitionPredicate.fromMultiple(partitionType, binaryPartitions);
        } else {
            // partitions may be partial partition fields, so here must to use predicate way.
            Predicate predicate =
                    partitions.stream()
                            .map(
                                    partition ->
                                            createPartitionPredicate(
                                                    partition, partitionType, partitionDefaultName))
                            .reduce(PredicateBuilder::or)
                            .orElseThrow(
                                    () -> new RuntimeException("Failed to get partition filter."));
            partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        }

        tryOverwritePartition(
                partitionFilter,
                emptyList(),
                emptyList(),
                commitIdentifier,
                null,
                new HashMap<>(),
                new HashMap<>());
    }

    @Override
    public void truncateTable(long commitIdentifier) {
        tryOverwritePartition(
                null,
                emptyList(),
                emptyList(),
                commitIdentifier,
                null,
                new HashMap<>(),
                new HashMap<>());
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        DataFilePathFactories factories = new DataFilePathFactories(pathFactory);
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory = factories.get(message.partition(), message.bucket());
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file));
            }
        }
    }

    @Override
    public FileStoreCommit withMetrics(CommitMetrics metrics) {
        this.commitMetrics = metrics;
        return this;
    }

    @Override
    public void commitStatistics(Statistics stats, long commitIdentifier) {
        String statsFileName = statsFileHandler.writeStats(stats);
        tryCommit(
                provider(emptyList(), emptyList(), emptyList()),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                provider(CommitKind.ANALYZE),
                noConflictCheck(),
                statsFileName);
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    private int tryCommit(
            ChangesProvider changesProvider,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties,
            CommitKindProvider commitKindProvider,
            ConflictCheck conflictCheck,
            @Nullable String statsFileName) {
        int retryCount = 0;
        RetryResult retryResult = null;
        long startMillis = System.currentTimeMillis();
        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();
            CommitChanges changes = changesProvider.provide(latestSnapshot);
            CommitKind commitKind = commitKindProvider.provide(changes);
            CommitResult result =
                    tryCommitOnce(
                            retryResult,
                            changes.tableFiles(),
                            changes.changelogFiles(),
                            changes.indexFiles(),
                            identifier,
                            watermark,
                            logOffsets,
                            properties,
                            commitKind,
                            latestSnapshot,
                            conflictCheck,
                            statsFileName);

            if (result.isSuccess()) {
                break;
            }

            retryResult = (RetryResult) result;

            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                String message =
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount);
                throw new RuntimeException(message, retryResult.exception());
            }

            commitRetryWait(retryCount);
            retryCount++;
        }
        return retryCount + 1;
    }

    /**
     * Try to overwrite partition.
     *
     * @param partitionFilter Partition filter indicating which partitions to overwrite, if {@code
     *     null}, overwrites the entire table.
     */
    private int tryOverwritePartition(
            @Nullable PartitionPredicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties) {
        CommitKindProvider commitKindProvider =
                commitChanges ->
                        containsFileDeletionOrDeletionVectors(
                                        commitChanges.tableFiles(), commitChanges.indexFiles())
                                ? CommitKind.OVERWRITE
                                : CommitKind.APPEND;
        return tryCommit(
                latestSnapshot ->
                        overwriteChanges(changes, indexFiles, latestSnapshot, partitionFilter),
                identifier,
                watermark,
                logOffsets,
                properties,
                commitKindProvider,
                mustConflictCheck(),
                null);
    }

    private CommitChanges overwriteChanges(
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            @Nullable Snapshot latestSnapshot,
            @Nullable PartitionPredicate partitionFilter) {
        List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
        List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
        if (latestSnapshot != null) {
            scan.withSnapshot(latestSnapshot)
                    .withPartitionFilter(partitionFilter)
                    .withKind(ScanMode.ALL);
            if (numBucket != BucketMode.POSTPONE_BUCKET) {
                // bucket = -2 can only be overwritten in postpone bucket tables
                scan.withBucketFilter(bucket -> bucket >= 0);
            }
            List<ManifestEntry> currentEntries = scan.plan().files();
            for (ManifestEntry entry : currentEntries) {
                changesWithOverwrite.add(
                        ManifestEntry.create(
                                FileKind.DELETE,
                                entry.partition(),
                                entry.bucket(),
                                entry.totalBuckets(),
                                entry.file()));
            }

            // collect index files
            if (latestSnapshot.indexManifest() != null) {
                List<IndexManifestEntry> entries =
                        indexManifestFile.read(latestSnapshot.indexManifest());
                for (IndexManifestEntry entry : entries) {
                    if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                        indexChangesWithOverwrite.add(entry.toDeleteEntry());
                    }
                }
            }
        }
        changesWithOverwrite.addAll(changes);
        indexChangesWithOverwrite.addAll(indexFiles);
        return new CommitChanges(changesWithOverwrite, emptyList(), indexChangesWithOverwrite);
    }

    @VisibleForTesting
    CommitResult tryCommitOnce(
            @Nullable RetryResult retryResult,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties,
            CommitKind commitKind,
            @Nullable Snapshot latestSnapshot,
            ConflictCheck conflictCheck,
            @Nullable String newStatsFileName) {
        long startMillis = System.currentTimeMillis();

        // Check if the commit has been completed. At this point, there will be no more repeated
        // commits and just return success
        if (retryResult != null && latestSnapshot != null) {
            Map<Long, Snapshot> snapshotCache = new HashMap<>();
            snapshotCache.put(latestSnapshot.id(), latestSnapshot);
            long startCheckSnapshot = Snapshot.FIRST_SNAPSHOT_ID;
            if (retryResult.latestSnapshot() != null) {
                snapshotCache.put(retryResult.latestSnapshot().id(), retryResult.latestSnapshot());
                startCheckSnapshot = retryResult.latestSnapshot().id() + 1;
            }
            for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
                Snapshot snapshot = snapshotCache.computeIfAbsent(i, snapshotManager::snapshot);
                if (snapshot.commitUser().equals(commitUser)
                        && snapshot.commitIdentifier() == identifier
                        && snapshot.commitKind() == commitKind) {
                    return new SuccessResult();
                }
            }
        }

        long newSnapshotId = Snapshot.FIRST_SNAPSHOT_ID;
        long firstRowIdStart = 0;
        if (latestSnapshot != null) {
            newSnapshotId = latestSnapshot.id() + 1;
            Long nextRowId = latestSnapshot.nextRowId();
            if (nextRowId != null) {
                firstRowIdStart = nextRowId;
            }
        }

        if (strictModeLastSafeSnapshot != null && strictModeLastSafeSnapshot >= 0) {
            conflictDetection.commitStrictModeCheck(
                    strictModeLastSafeSnapshot, newSnapshotId, commitKind, snapshotManager);
            strictModeLastSafeSnapshot = newSnapshotId - 1;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit table files to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : deltaFiles) {
                LOG.debug("  * {}", entry);
            }
            LOG.debug("Ready to commit changelog to snapshot {}", newSnapshotId);
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * {}", entry);
            }
        }

        List<SimpleFileEntry> baseDataFiles = new ArrayList<>();
        boolean discardDuplicate = discardDuplicateFiles && commitKind == CommitKind.APPEND;
        if (latestSnapshot != null
                && (discardDuplicate || conflictCheck.shouldCheck(latestSnapshot.id()))) {
            // latestSnapshotId is different from the snapshot id we've checked for conflicts,
            // so we have to check again
            List<BinaryRow> changedPartitions =
                    commitScanner.changedPartitions(
                            deltaFiles, Collections.emptyList(), indexFiles);
            if (retryResult != null && retryResult.latestSnapshot() != null) {
                baseDataFiles = new ArrayList<>(retryResult.baseDataFiles());
                List<SimpleFileEntry> incremental =
                        commitScanner.readIncrementalChanges(
                                retryResult.latestSnapshot(), latestSnapshot, changedPartitions);
                if (!incremental.isEmpty()) {
                    baseDataFiles.addAll(incremental);
                    baseDataFiles = new ArrayList<>(FileEntry.mergeEntries(baseDataFiles));
                }
            } else {
                baseDataFiles =
                        commitScanner.readAllEntriesFromChangedPartitions(
                                latestSnapshot, changedPartitions);
            }
            if (discardDuplicate) {
                Set<FileEntry.Identifier> baseIdentifiers =
                        baseDataFiles.stream()
                                .map(FileEntry::identifier)
                                .collect(Collectors.toSet());
                deltaFiles =
                        deltaFiles.stream()
                                .filter(entry -> !baseIdentifiers.contains(entry.identifier()))
                                .collect(Collectors.toList());
            }
            conflictDetection.checkNoConflictsOrFail(
                    latestSnapshot,
                    baseDataFiles,
                    SimpleFileEntry.from(deltaFiles),
                    indexFiles,
                    commitKind);
        }

        Snapshot newSnapshot;
        ManifestAccumulator.Result manifestResult = null;
        try {
            manifestResult =
                    manifestAccumulator.accumulate(
                            latestSnapshot,
                            deltaFiles,
                            changelogFiles,
                            indexFiles,
                            watermark,
                            logOffsets,
                            newSnapshotId,
                            firstRowIdStart);

            newSnapshot =
                    snapshotBuilder.build(
                            latestSnapshot,
                            identifier,
                            commitKind,
                            logOffsets,
                            properties,
                            newStatsFileName,
                            newSnapshotId,
                            manifestResult,
                            changelogFiles);
        } catch (Throwable e) {
            // fails when preparing for commit, we should clean up
            if (manifestResult != null) {
                manifestAccumulator.cleanUpReuseTmpManifests(
                        manifestResult.deltaManifestList(),
                        manifestResult.changelogManifestList(),
                        manifestResult.oldIndexManifest(),
                        manifestResult.indexManifest());
                manifestAccumulator.cleanUpNoReuseTmpManifests(
                        manifestResult.baseManifestList(),
                        manifestResult.mergeBeforeManifests(),
                        manifestResult.mergeAfterManifests());
            }
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId, commitUser, identifier, commitKind.name()),
                    e);
        }

        boolean success;
        try {
            success = commitSnapshotImpl(newSnapshot, manifestResult.deltaStatistics());
        } catch (Exception e) {
            // commit exception, not sure about the situation and should not clean up the files
            LOG.warn("Retry commit for exception.", e);
            return new RetryResult(latestSnapshot, baseDataFiles, e);
        }

        if (!success) {
            // commit fails, should clean up the files
            long commitTime = (System.currentTimeMillis() - startMillis) / 1000;
            LOG.warn(
                    "Atomic commit failed for snapshot #{} by user {} "
                            + "with identifier {} and kind {} after {} seconds. "
                            + "Clean up and try again.",
                    newSnapshotId,
                    commitUser,
                    identifier,
                    commitKind.name(),
                    commitTime);
            manifestAccumulator.cleanUpNoReuseTmpManifests(
                    manifestResult.baseManifestList(),
                    manifestResult.mergeBeforeManifests(),
                    manifestResult.mergeAfterManifests());
            return new RetryResult(latestSnapshot, baseDataFiles, null);
        }

        LOG.info(
                "Successfully commit snapshot {} to table {} by user {} "
                        + "with identifier {} and kind {}.",
                newSnapshotId,
                tableName,
                commitUser,
                identifier,
                commitKind.name());
        if (strictModeLastSafeSnapshot != null) {
            strictModeLastSafeSnapshot = newSnapshot.id();
        }
        final List<SimpleFileEntry> finalBaseFiles = baseDataFiles;
        final List<ManifestEntry> finalDeltaFiles = manifestResult.deltaFiles();
        commitCallbacks.forEach(
                callback ->
                        callback.call(finalBaseFiles, finalDeltaFiles, indexFiles, newSnapshot));
        return new SuccessResult();
    }

    public boolean replaceManifestList(
            Snapshot latest,
            long totalRecordCount,
            Pair<String, Long> baseManifestList,
            Pair<String, Long> deltaManifestList) {
        Snapshot newSnapshot =
                new Snapshot(
                        latest.id() + 1,
                        latest.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getLeft(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latest.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.OVERWRITE,
                        System.currentTimeMillis(),
                        latest.logOffsets(),
                        totalRecordCount,
                        0L,
                        0L,
                        latest.watermark(),
                        latest.statistics(),
                        // if empty properties, just set to null
                        latest.properties(),
                        latest.nextRowId());

        return commitSnapshotImpl(newSnapshot, Collections.emptyList());
    }

    public void compactManifest() {
        int retryCount = 0;
        long startMillis = System.currentTimeMillis();
        while (true) {
            boolean success = compactManifestOnce();
            if (success) {
                break;
            }

            if (System.currentTimeMillis() - startMillis > commitTimeout
                    || retryCount >= commitMaxRetries) {
                throw new RuntimeException(
                        String.format(
                                "Commit failed after %s millis with %s retries, there maybe exist commit conflicts between multiple jobs.",
                                commitTimeout, retryCount));
            }

            commitRetryWait(retryCount);
            retryCount++;
        }
    }

    private boolean compactManifestOnce() {
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        if (latestSnapshot == null) {
            return true;
        }

        List<ManifestFileMeta> mergeBeforeManifests =
                manifestList.readDataManifests(latestSnapshot);
        List<ManifestFileMeta> mergeAfterManifests;

        // the fist trial
        mergeAfterManifests =
                ManifestFileMerger.merge(
                        mergeBeforeManifests,
                        manifestFile,
                        manifestTargetSize.getBytes(),
                        1,
                        1,
                        partitionType,
                        manifestReadParallelism);

        if (new HashSet<>(mergeBeforeManifests).equals(new HashSet<>(mergeAfterManifests))) {
            // no need to commit this snapshot, because no compact were happened
            return true;
        }

        Pair<String, Long> baseManifestList = manifestList.write(mergeAfterManifests);
        Pair<String, Long> deltaManifestList = manifestList.write(emptyList());

        // prepare snapshot file
        Snapshot newSnapshot =
                new Snapshot(
                        latestSnapshot.id() + 1,
                        latestSnapshot.schemaId(),
                        baseManifestList.getLeft(),
                        baseManifestList.getRight(),
                        deltaManifestList.getLeft(),
                        deltaManifestList.getRight(),
                        null,
                        null,
                        latestSnapshot.indexManifest(),
                        commitUser,
                        Long.MAX_VALUE,
                        CommitKind.COMPACT,
                        System.currentTimeMillis(),
                        latestSnapshot.logOffsets(),
                        latestSnapshot.totalRecordCount(),
                        0L,
                        0L,
                        latestSnapshot.watermark(),
                        latestSnapshot.statistics(),
                        latestSnapshot.properties(),
                        latestSnapshot.nextRowId());

        return commitSnapshotImpl(newSnapshot, emptyList());
    }

    private boolean commitSnapshotImpl(Snapshot newSnapshot, List<PartitionEntry> deltaStatistics) {
        try {
            List<PartitionStatistics> statistics = new ArrayList<>(deltaStatistics.size());
            for (PartitionEntry entry : deltaStatistics) {
                statistics.add(entry.toPartitionStatistics(partitionComputer));
            }
            return snapshotCommit.commit(newSnapshot, branchName, statistics);
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshot.id(),
                            commitUser,
                            newSnapshot.commitIdentifier(),
                            newSnapshot.commitKind().name()),
                    e);
        }
    }

    private void commitRetryWait(int retryCount) {
        int retryWait =
                (int) Math.min(commitMinRetryWait * Math.pow(2, retryCount), commitMaxRetryWait);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        retryWait += random.nextInt(Math.max(1, (int) (retryWait * 0.2)));
        try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void close() {
        for (CommitCallback callback : commitCallbacks) {
            IOUtils.closeQuietly(callback);
        }
        IOUtils.closeQuietly(snapshotCommit);
    }

    private static ChangesProvider provider(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles) {
        return s -> new CommitChanges(tableFiles, changelogFiles, indexFiles);
    }

    @FunctionalInterface
    private interface ChangesProvider {
        CommitChanges provide(@Nullable Snapshot latestSnapshot);
    }

    @FunctionalInterface
    private interface CommitKindProvider {
        CommitKind provide(CommitChanges changes);
    }

    private static CommitKindProvider provider(CommitKind kind) {
        return changes -> kind;
    }
}
