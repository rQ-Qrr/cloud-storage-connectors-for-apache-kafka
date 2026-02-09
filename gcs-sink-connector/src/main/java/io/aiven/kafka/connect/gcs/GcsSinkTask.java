/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.gcs;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.kafka.connect.common.grouper.RecordGrouper;
import io.aiven.kafka.connect.common.grouper.RecordGrouperFactory;
import io.aiven.kafka.connect.common.output.OutputWriter;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkTask.class);
    private static final String USER_AGENT_HEADER_KEY = "user-agent";

    private RecordGrouper recordGrouper;

    private GcsSinkConfig config;

    private Storage storage;

    private TopicPartitionManager topicPartitionManager;

    private long gcsRecordsBufferedBytesSoftLimit;

    private long currentRecordsBufferedBytes;

    // required by Connect
    public GcsSinkTask() {
        super();
    }

    // for testing
    public GcsSinkTask(final Map<String, String> props, final Storage storage) {
        super();
        Objects.requireNonNull(props, "props cannot be null");
        Objects.requireNonNull(storage, "storage cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = storage;
        initRest();
        this.gcsRecordsBufferedBytesSoftLimit = config.getGcsRecordsBufferedBytesSoftLimit();
        this.topicPartitionManager = new TopicPartitionManager();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.config = new GcsSinkConfig(props);
        this.storage = StorageOptions.newBuilder()
                .setHost(config.getGcsEndpoint())
                .setCredentials(config.getCredentials())
                .setHeaderProvider(FixedHeaderProvider.create(USER_AGENT_HEADER_KEY, config.getUserAgent()))
                .setRetrySettings(RetrySettings.newBuilder()
                        .setInitialRetryDelayDuration(config.getGcsRetryBackoffInitialDelay())
                        .setMaxRetryDelayDuration(config.getGcsRetryBackoffMaxDelay())
                        .setRetryDelayMultiplier(config.getGcsRetryBackoffDelayMultiplier())
                        .setTotalTimeoutDuration(config.getGcsRetryBackoffTotalTimeout())
                        .setMaxAttempts(config.getGcsRetryBackoffMaxAttempts())
                        .build())
                .build()
                .getService();
        initRest();
        if (Objects.nonNull(config.getKafkaRetryBackoffMs())) {
            context.timeout(config.getKafkaRetryBackoffMs());
        }
        this.gcsRecordsBufferedBytesSoftLimit = config.getGcsRecordsBufferedBytesSoftLimit();
        this.topicPartitionManager = new TopicPartitionManager();
    }

    private void initRest() {
        try {
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
        } catch (final Exception e) { // NOPMD broad exception caught
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");

        LOG.debug("Processing {} records", records.size());
        for (final SinkRecord record : records) {
            // Calculate byte size of the record value and add to total buffered size
            this.currentRecordsBufferedBytes += getRecordValueByteSize(record.value());
            recordGrouper.put(record);
        }
        checkRecordSize();
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            recordGrouper.records().forEach(this::flushFile);
        } finally {
            recordGrouper.clear();
        }
        currentRecordsBufferedBytes = 0;
        topicPartitionManager.resumeAll();
    }

    private void flushFile(final String filename, final List<SinkRecord> records) {
        final BlobInfo blob = BlobInfo.newBuilder(config.getBucketName(), config.getPrefix() + filename)
                .setContentEncoding(config.getObjectContentEncoding())
                .build();
        try (var out = Channels.newOutputStream(storage.writer(blob));
                var writer = OutputWriter.builder()
                        .withExternalProperties(config.originalsStrings())
                        .withOutputFields(config.getOutputFields())
                        .withCompressionType(config.getCompressionType())
                        .withEnvelopeEnabled(config.envelopeEnabled())
                        .build(out, config.getFormatType())) {
            writer.writeRecords(records);
        } catch (final Exception e) { // NOPMD broad exception caught
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        // Nothing to do.
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    // Helper to get the byte size of a SinkRecord value.
    private long getRecordValueByteSize(final Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof byte[]) {
            // If the value is a byte array, its length is the byte size.
            return ((byte[]) value).length;
        }
        if (value instanceof ByteBuffer) {
            // If the value is a ByteBuffer, use the number of remaining bytes.
            return ((ByteBuffer) value).remaining();
        }
        // For other types, convert to a String and estimate the UTF-8 byte length.
        // This is a reasonable estimate, assuming the OutputWriter serializes these
        // values in a way consistent with UTF-8 encoding.
        final String stringValue = value.toString();
        return Utf8.encodedLength(stringValue);
    }

    // Important: this method is only safe to call during put(), flush(), or preCommit(); otherwise,
    // a ConcurrentModificationException may be triggered if the Connect framework is in the middle of
    // a method invocation on the consumer for this task. This becomes especially likely if all topics
    // have been paused as the framework will most likely be in the middle of a poll for that consumer
    // which, because all of its topics have been paused, will not return until it's time for the next
    // offset commit. Invoking context.requestCommit() won't wake up the consumer in that case, so we
    // really have no choice but to wait for the framework to call a method on this task that implies
    // that it's safe to pause or resume partitions on the consumer.
    private void checkRecordSize() {
        if (gcsRecordsBufferedBytesSoftLimit != 0) {
            LOG.debug("Record soft limit: {} bytes, current record size: {} bytes", gcsRecordsBufferedBytesSoftLimit,
                    currentRecordsBufferedBytes);
            if (currentRecordsBufferedBytes > gcsRecordsBufferedBytesSoftLimit && !topicPartitionManager.isPaused) {
                topicPartitionManager.pauseAll();
                topicPartitionManager.requestCommit();
            } else if (currentRecordsBufferedBytes <= gcsRecordsBufferedBytesSoftLimit / 2) {
                // resume only if there is a reasonable chance we won't immediately have to pause again.
                topicPartitionManager.resumeAll();
            }
        }
    }

    private class TopicPartitionManager {

        private Long lastChangeMs;
        private Long lastCommitMs;
        private boolean isPaused;

        public TopicPartitionManager() {
            this.lastChangeMs = System.currentTimeMillis();
            this.lastCommitMs = System.currentTimeMillis();
            this.isPaused = false;
        }

        public void pauseAll() {
            if (!isPaused) {
                final long now = System.currentTimeMillis();
                LOG.debug("Paused all partitions after {}ms", now - lastChangeMs);
                isPaused = true;
                lastChangeMs = now;
            }
            final Set<TopicPartition> assignment = context.assignment();
            final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
            context.pause(assignment.toArray(topicPartitions));
        }

        public void resumeAll() {
            if (isPaused) {
                final long now = System.currentTimeMillis();
                LOG.debug("Resumed all partitions after {}ms", now - lastChangeMs);
                isPaused = false;
                lastChangeMs = now;
                final Set<TopicPartition> assignment = context.assignment();
                final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
                context.resume(assignment.toArray(topicPartitions));
            }
        }

        public void requestCommit() {
            final long now = System.currentTimeMillis();
            LOG.debug("Requesting commit for all partitions after {}ms", now - lastCommitMs);
            lastCommitMs = now;
            context.requestCommit();
        }
    }
}
