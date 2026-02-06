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

import io.aiven.kafka.connect.common.grouper.TopicPartitionKeyRecordGrouper;
import io.aiven.kafka.connect.common.grouper.TopicPartitionRecordGrouper;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
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
import java.io.IOException;
import java.io.OutputStream;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(GcsSinkTask.class);
    private static final String USER_AGENT_HEADER_KEY = "user-agent";

    private RecordGrouper recordGrouper;

    private GcsSinkConfig config;

    private Storage storage;

    private TopicPartitionManager topicPartitionManager;
    // Helper class to manage GCS Resumable Uploads
    private final Map<String, GcsBlobWriter> activeBlobWriters = new HashMap<>();

    private final long GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK = 60 * 1024 * 1024L; // 60 MiB
    private long currentBufferedBytes = 0;
    private long lastWriteMs;

    private boolean isOneRecordPerFile;

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
    }

    private void initRest() {
        try {
            this.topicPartitionManager = new TopicPartitionManager();
            this.recordGrouper = RecordGrouperFactory.newRecordGrouper(config);
            final String grType = RecordGrouperFactory.resolveRecordGrouperType(config.getFilenameTemplate());
            if (RecordGrouperFactory.KEY_RECORD.equals(grType) ||
                RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD.equals(grType)) {
                this.isOneRecordPerFile = true;
            }
            this.lastWriteMs = System.currentTimeMillis();
        } catch (final Exception e) { // NOPMD broad exception caught
            throw new ConnectException("Unsupported file name template " + config.getFilename(), e);
        }
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        Objects.requireNonNull(records, "records cannot be null");
        LOG.debug(
            "Buffering {} records. Current buffer size: {} bytes",
            records.size(),
            currentBufferedBytes);
        for (final SinkRecord record : records) {
            recordGrouper.put(record);
            if (!isOneRecordPerFile) {
                currentBufferedBytes += estimateRecordSize(record);
            }
        }

        if(isOneRecordPerFile) return;

        // check if we should pause topics
        checkRecordSize();
        final long GCS_WRITE_INTERVAL_MS = 10000L; // 10 seconds
        // Trigger write to GCS if the buffer size threshold is reached or the time interval has passed.
        if (currentBufferedBytes >= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK
            || System.currentTimeMillis() - lastWriteMs >= GCS_WRITE_INTERVAL_MS) {
            if (currentBufferedBytes >= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
                LOG.debug(
                    "GCS write buffer size of {} bytes reached. Writing buffered records to GCS.",
                    currentBufferedBytes);
            } else {
                LOG.debug("GCS write interval of {} ms reached. Writing buffered records to GCS.",
                    GCS_WRITE_INTERVAL_MS);
            }
            writeBufferedRecordsToGcs();
        }
        // check if we should resume topics
        checkRecordSize();
    }

    // Estimates the size of a SinkRecord in bytes. This is a rough approximation based on the byte
    // length of the key and value's String representation. The actual size written to GCS can vary
    // due to serialization format and compression.
    private long estimateRecordSize(SinkRecord record) {
        long size = 0;
        if (record.key() != null) {
            size += getObjectSize(record.key());
        }
        if (record.value() != null) {
            size += getObjectSize(record.value());
        }
        // Add a small constant overhead for record metadata
        size += 20;
        return size;
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOG.debug("Flush triggered. Writing any remaining buffered records and closing GCS files.");

        // Write any records still in the buffer that didn't meet thresholds.
        if (currentBufferedBytes > 0 || isOneRecordPerFile) {
            LOG.debug("Writing remaining {} buffered bytes during flush.", currentBufferedBytes);
            try {
                writeBufferedRecordsToGcs();
            } catch (Exception e) {
                LOG.error("Failed to write remaining buffered records during flush: {}", e.getMessage());
                throw new ConnectException("Failed to write buffered records during flush: " + e.getMessage(), e);
            }
        }

        ConnectException firstException = null;
        // Close all active GcsBlobWriters, which finalizes the resumable uploads in GCS.
        for (Map.Entry<String, GcsBlobWriter> entry : activeBlobWriters.entrySet()) {
            String fullPath = entry.getKey();
            GcsBlobWriter blobWriter = entry.getValue();
            try {
                LOG.debug("Closing GcsBlobWriter for: gs://{}/{}", config.getBucketName(), fullPath);
                blobWriter.close(); // This finalizes the GCS object.
            } catch (final Exception e) {
                LOG.error(
                    "Error closing GCS file gs://{}/{}: {}",
                    config.getBucketName(),
                    fullPath,
                    e.getMessage());
                if (firstException == null) {
                    firstException = new ConnectException("Failed to close GCS file " + fullPath, e);
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        // Clear the map of active writers.
        activeBlobWriters.clear();
        // recordGrouper should be empty after writeBufferedRecordsToGcs(), but clear again for safety.
        recordGrouper.clear();

        if (firstException != null) {
            // If any file failed to close, throw an exception to prevent offset commit.
            throw firstException;
        }
        LOG.debug("Successfully flushed and closed all GCS files.");

        topicPartitionManager.resumeAll();
    }

    @Override
    public void stop() {
        LOG.info("Stopping GcsSinkTask. Attempting to close any remaining active GcsBlobWriters.");
        for (GcsBlobWriter blobWriter : activeBlobWriters.values()) {
            try {
                blobWriter.close();
            } catch (IOException e) {
                LOG.warn("Error closing GcsBlobWriter during stop: {}", e.getMessage());
            }
        }
        activeBlobWriters.clear();
        recordGrouper.clear();
        currentBufferedBytes = 0;
        lastWriteMs = System.currentTimeMillis();
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    private void writeBufferedRecordsToGcs() {
        ConnectException firstException = null;
        Map<String, List<SinkRecord>> recordsToWrite = recordGrouper.records();

        if (recordsToWrite.isEmpty()) {
            LOG.trace("No records buffered to write.");
            return;
        }

        LOG.debug("Writing buffered records for {} files to GCS.", recordsToWrite.size());
        for (Map.Entry<String, List<SinkRecord>> entry : recordsToWrite.entrySet()) {
            final String filename = entry.getKey();
            final List<SinkRecord> records = entry.getValue();

            GcsBlobWriter blobWriter;
            try {
                // computeIfAbsent's mappingFunction can throw checked exceptions,
                // so we need to handle IOException from the GcsBlobWriter constructor
                blobWriter =
                    activeBlobWriters.computeIfAbsent(
                        filename,
                        k -> {
                            LOG.info(
                                "Creating new GcsBlobWriter (Resumable Upload) for: gs://{}/{}",
                                config.getBucketName(),
                                k);
                            try {
                                return new GcsBlobWriter(storage, config, k);
                            } catch (IOException e) {
                                // Wrap IOException in ConnectException as Function doesn't allow checked
                                // exceptions
                                throw new ConnectException("Failed to initialize GcsBlobWriter for " + k, e);
                            }
                        });
                // Write the current records to the BlobWriter. This can also throw IOException.
                blobWriter.writeRecords(records);
            } catch (final ConnectException e) {
                // Catch ConnectExceptions thrown from the lambda
                LOG.error(
                    "Error during GCS writer initialization or write for file gs://{}/{} : {}",
                    config.getBucketName(),
                    config.getPrefix() + filename,
                    e.getMessage());
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            } catch (final IOException e) {
                // Catch IOExceptions thrown directly from blobWriter.writeRecords(records)
                LOG.error(
                    "Error writing records to GCS for file gs://{}/{} : {}",
                    config.getBucketName(),
                    config.getPrefix() + filename,
                    e.getMessage());
                ConnectException ce =
                    new ConnectException("Failed to write records to GCS for " + config.getPrefix() + filename, e);
                if (firstException == null) {
                    firstException = ce;
                } else {
                    firstException.addSuppressed(ce);
                }
            } catch (final Exception e) { // Catch any other unexpected exceptions
                LOG.error(
                    "Unexpected error during GCS write for file gs://{}/{} : {}",
                    config.getBucketName(),
                    config.getPrefix() + filename,
                    e.getMessage());
                ConnectException ce =
                    new ConnectException("Unexpected error during GCS write for " + config.getPrefix() + filename, e);
                if (firstException == null) {
                    firstException = ce;
                } else {
                    firstException.addSuppressed(ce);
                }
            }
        }

        if (firstException == null) {
            LOG.debug("Successfully wrote all buffered records to GCS channels. Clearing grouper.");
            if (recordGrouper instanceof TopicPartitionRecordGrouper) {
                ((TopicPartitionRecordGrouper) recordGrouper).clearFileBuffers();
                LOG.info("Cleared file buffers for TopicPartitionRecordGrouper");
            } else if (recordGrouper instanceof TopicPartitionKeyRecordGrouper) {
                ((TopicPartitionKeyRecordGrouper) recordGrouper).clearFileBuffers();
                LOG.info("Cleared file buffers for TopicPartitionKeyRecordGrouper");
            } else {
                recordGrouper.clear();
                LOG.info("Cleared file buffers for RecordGrouper");
            }
            this.currentBufferedBytes = 0; // Reset after successful write
            this.lastWriteMs = System.currentTimeMillis();
        } else {
            LOG.error("One or more errors occurred while writing buffered records to GCS.");
            // Re-throw to signal Kafka Connect to retry the batch.
            throw firstException;
        }
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
        LOG.debug(
            "Record soft limit: {} bytes, current record size: {} bytes",
            GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK,
            currentBufferedBytes);
        if (currentBufferedBytes > GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK) {
            topicPartitionManager.pauseAll();
        } else if (currentBufferedBytes <= GCS_WRITE_BUFFER_SIZE_BYTES_PER_TASK / 2) {
            // resume only if there is a reasonable chance we won't immediately have to pause again.
            topicPartitionManager.resumeAll();
        }
    }

    private long getObjectSize(Object data) {
        if (data instanceof byte[]) {
            return ((byte[]) data).length;
        }
        else if (data instanceof String) {
            return ((String) data).getBytes(StandardCharsets.UTF_8).length;
        }
        else {
            try {
                return data.toString().getBytes(StandardCharsets.UTF_8).length;
            } catch (Exception e) {
                LOG.trace("Could not estimate size of record data: {}", e.getMessage());
                // Return a fallback size (e.g., 100 bytes) to ensure the buffer eventually flushes
                // preventing a potential infinite memory buildup.
                return 100;
            }
        }
    }

    /**
     * Helper class to manage a GCS WriteChannel and its associated OutputWriter for a single GCS
     * object, using resumable uploads.
     */
    private static class GcsBlobWriter implements AutoCloseable {
        private final WriteChannel channel;
        private final OutputWriter outputWriter;
        private final String fullPath;
        private final String bucketName;

        public GcsBlobWriter(Storage storage, GcsSinkConfig config, String filename)
            throws IOException {
            this.bucketName = config.getBucketName();
            this.fullPath = config.getPrefix() + filename;
            BlobInfo blob =
                BlobInfo.newBuilder(bucketName, fullPath)
                    .setContentEncoding(config.getObjectContentEncoding())
                    .build();
            // storage.writer() initiates a GCS resumable upload.
            this.channel = storage.writer(blob);
            OutputStream out = Channels.newOutputStream(channel);
            this.outputWriter =
                OutputWriter.builder()
                    .withExternalProperties(config.originalsStrings())
                    .withOutputFields(config.getOutputFields())
                    .withCompressionType(config.getCompressionType())
                    .withEnvelopeEnabled(config.envelopeEnabled())
                    .build(out, config.getFormatType()); // This line can throw IOException
        }

        // Writes a list of SinkRecords to the OutputWriter, streaming data to the GCS WriteChannel.
        public void writeRecords(List<SinkRecord> records) throws IOException {
            LOG.debug("Writing {} records to gs://{}/{}", records.size(), bucketName, fullPath);
            outputWriter.writeRecords(records);
        }

        @Override
        public void close() throws IOException {
            LOG.debug("Closing OutputWriter and WriteChannel for gs://{}/{}", bucketName, fullPath);
            try {
                outputWriter.close();
            } finally {
                // Ensure the channel is closed even if outputWriter.close() throws.
                // Closing the channel finalizes the resumable upload in GCS.
                channel.close();
            }
        }
    }

    private class TopicPartitionManager {

        private Long lastChangeMs;
        private boolean isPaused;

        public TopicPartitionManager() {
            this.lastChangeMs = System.currentTimeMillis();
            this.isPaused = false;
        }

        public void pauseAll() {
            if (!isPaused) {
                long now = System.currentTimeMillis();
                LOG.debug("Paused all partitions after {}ms", now - lastChangeMs);
                isPaused = true;
                lastChangeMs = now;
            }
            Set<TopicPartition> assignment = context.assignment();
            final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
            context.pause(assignment.toArray(topicPartitions));
        }

        public void resumeAll() {
            if (isPaused) {
                long now = System.currentTimeMillis();
                LOG.debug("Resumed all partitions after {}ms", now - lastChangeMs);
                isPaused = false;
                lastChangeMs = now;
                Set<TopicPartition> assignment = context.assignment();
                final TopicPartition[] topicPartitions = new TopicPartition[assignment.size()];
                context.resume(assignment.toArray(topicPartitions));
            }
        }
    }
}
