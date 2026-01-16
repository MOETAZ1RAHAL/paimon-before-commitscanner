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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Collector for {@link CommitMessage}s. */
public class CommitMessageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(CommitMessageCollector.class);

    private final int numBucket;
    private final CoreOptions options;
    private final ConflictDetection conflictDetection;

    public CommitMessageCollector(
            int numBucket, CoreOptions options, ConflictDetection conflictDetection) {
        this.numBucket = numBucket;
        this.options = options;
        this.conflictDetection = conflictDetection;
    }

    public void collect(
            List<CommitMessage> commitMessages,
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<IndexManifestEntry> appendIndexFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog,
            List<IndexManifestEntry> compactIndexFiles) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            commitMessage
                    .newFilesIncrement()
                    .newFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .newFilesIncrement()
                    .deletedFiles()
                    .forEach(
                            m ->
                                    appendTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
            commitMessage
                    .newFilesIncrement()
                    .changelogFiles()
                    .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .newFilesIncrement()
                    .deletedIndexFiles()
                    .forEach(
                            m ->
                                    appendIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.DELETE,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    m)));
            commitMessage
                    .newFilesIncrement()
                    .newIndexFiles()
                    .forEach(
                            m ->
                                    appendIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.ADD,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    m)));

            commitMessage
                    .compactIncrement()
                    .compactBefore()
                    .forEach(
                            m ->
                                    compactTableFiles.add(
                                            makeEntry(FileKind.DELETE, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .compactAfter()
                    .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .changelogFiles()
                    .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
            commitMessage
                    .compactIncrement()
                    .deletedIndexFiles()
                    .forEach(
                            m ->
                                    compactIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.DELETE,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    m)));
            commitMessage
                    .compactIncrement()
                    .newIndexFiles()
                    .forEach(
                            m ->
                                    compactIndexFiles.add(
                                            new IndexManifestEntry(
                                                    FileKind.ADD,
                                                    commitMessage.partition(),
                                                    commitMessage.bucket(),
                                                    m)));
        }
        if (!commitMessages.isEmpty()) {
            List<String> msg = new ArrayList<>();
            if (!appendTableFiles.isEmpty()) {
                msg.add(appendTableFiles.size() + " append table files");
            }
            if (!appendChangelog.isEmpty()) {
                msg.add(appendChangelog.size() + " append Changelogs");
            }
            if (!appendIndexFiles.isEmpty()) {
                msg.add(appendIndexFiles.size() + " append index files");
            }
            if (!compactTableFiles.isEmpty()) {
                msg.add(compactTableFiles.size() + " compact table files");
            }
            if (!compactChangelog.isEmpty()) {
                msg.add(compactChangelog.size() + " compact Changelogs");
            }
            if (!compactIndexFiles.isEmpty()) {
                msg.add(compactIndexFiles.size() + " compact index files");
            }
            LOG.info("Finished collecting changes, including: {}", String.join(", ", msg));
        }
    }

    private ManifestEntry makeEntry(
            FileKind kind, CommitMessageImpl commitMessage, DataFileMeta file) {
        Integer totalBuckets = commitMessage.totalBuckets();
        if (totalBuckets == null) {
            totalBuckets = numBucket;
        }

        return ManifestEntry.create(
                kind, commitMessage.partition(), commitMessage.bucket(), totalBuckets, file);
    }

    public List<ManifestEntry> tryUpgrade(List<ManifestEntry> appendFiles) {
        if (!options.overwriteUpgrade()) {
            return appendFiles;
        }
        Comparator<InternalRow> keyComparator = conflictDetection.keyComparator();
        if (keyComparator == null) {
            return appendFiles;
        }
        for (ManifestEntry entry : appendFiles) {
            if (entry.level() > 0 || entry.bucket() < 0) {
                return appendFiles;
            }
        }

        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = new HashMap<>();
        for (ManifestEntry entry : appendFiles) {
            buckets.computeIfAbsent(
                            Pair.of(entry.partition(), entry.bucket()), k -> new ArrayList<>())
                    .add(entry);
        }

        List<ManifestEntry> results = new ArrayList<>();
        int maxLevel = options.numLevels() - 1;
        outer:
        for (List<ManifestEntry> entries : buckets.values()) {
            List<ManifestEntry> newEntries = new ArrayList<>(entries);
            newEntries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < newEntries.size(); i++) {
                ManifestEntry a = newEntries.get(i);
                ManifestEntry b = newEntries.get(i + 1);
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    results.addAll(entries);
                    continue outer;
                }
            }
            LOG.info("Upgraded for overwrite commit.");
            for (ManifestEntry entry : newEntries) {
                results.add(entry.upgrade(maxLevel));
            }
        }

        return results;
    }
}
