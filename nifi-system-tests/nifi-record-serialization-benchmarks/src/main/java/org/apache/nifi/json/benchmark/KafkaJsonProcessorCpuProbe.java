/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.json.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.service.Kafka3ConnectionService;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.kafka.shared.property.SchemaConflictResolution;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class KafkaJsonProcessorCpuProbe {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final int TARGET_RECORD_BYTES = 2_048;
    private static final int MAX_POLL_RECORDS = 1_000;
    private static final int WARMUP_RECORDS = 50_000;
    private static final int FETCH_MAX_WAIT_MILLIS = 20;
    private static final int MAX_UNCOMMITTED_MILLIS = 100;
    private static final long DRAIN_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(5);

    private KafkaJsonProcessorCpuProbe() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length < 5 || arguments.length > 7) {
            throw new IllegalArgumentException("Expected: MODE RECORDS PARTITIONS ITERATIONS OUTPUT_FILE [NULL_PERCENTAGE] [DRIFTING]");
        }

        final KafkaJsonReaderMode readerMode = KafkaJsonReaderMode.valueOf(arguments[0]);
        final int recordCount = positiveInteger(arguments[1], "records");
        final int partitionCount = positiveInteger(arguments[2], "partitions");
        final int iterationCount = positiveInteger(arguments[3], "iterations");
        final Path outputFile = Path.of(arguments[4]);
        final int nullPercentage = arguments.length >= 6 ? percentage(arguments[5]) : 0;
        final boolean drifting = arguments.length == 7 && Boolean.parseBoolean(arguments[6]);
        final String imageName = System.getProperty("kafka.docker.image", "apache/kafka:4.3.1");
        final String revision = requiredSystemProperty("probe.revision");
        final String runLabel = System.getProperty("probe.runLabel", outputFile.getFileName().toString());
        final String artifactSha256 = artifactSha256();

        try (KafkaContainer container = new KafkaContainer(DockerImageName.parse(imageName))) {
            container.start();
            final ProbeResult result = runProbe(container.getBootstrapServers(), imageName, readerMode, recordCount,
                    partitionCount, iterationCount, nullPercentage, drifting, revision, runLabel, artifactSha256);
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputFile.toFile(), result);
            for (final Measurement measurement : result.measurements()) {
                System.out.printf("%s iteration=%d processor-thread-records/cpu-second=%.0f processor-thread-cpu-us/record=%.3f "
                                + "process-cpu-us/record=%.3f wall-us/record=%.3f allocated-bytes/record=%.0f cores=%.3f gc-ms=%d%n",
                        readerMode, measurement.iteration(), measurement.recordsPerProcessorThreadCpuSecond(),
                        measurement.processorThreadCpuMicrosPerRecord(),
                        measurement.cpuMicrosPerRecord(), measurement.wallMicrosPerRecord(), measurement.allocatedBytesPerRecord(),
                        measurement.coreEquivalent(), measurement.gcMillis());
            }
        }
    }

    private static ProbeResult runProbe(final String bootstrapServers, final String imageName, final KafkaJsonReaderMode readerMode,
                                        final int recordCount, final int partitionCount, final int iterationCount,
                                        final int nullPercentage, final boolean drifting, final String revision,
                                        final String runLabel, final String artifactSha256) throws Exception {
        final String topic = "nifi-json-cpu-" + UUID.randomUUID();
        createTopic(bootstrapServers, topic, partitionCount);

        final ProbeRunner probeRunner = createRunner(bootstrapServers, topic, readerMode);
        final TestRunner runner = probeRunner.runner();
        try (KafkaProducer<byte[], byte[]> producer = createProducer(bootstrapServers)) {
            runner.run(1, false, true);
            produce(producer, topic, partitionCount, 0, WARMUP_RECORDS, nullPercentage, drifting);
            final OutputValidator outputValidator = new OutputValidator(0, WARMUP_RECORDS, nullPercentage, drifting);
            drain(runner, WARMUP_RECORDS, outputValidator);
            final ValidationSummary validation = outputValidator.finish();

            final List<Measurement> measurements = new ArrayList<>(iterationCount);
            long sequence = WARMUP_RECORDS;
            for (int iteration = 1; iteration <= iterationCount; iteration++) {
                produce(producer, topic, partitionCount, sequence, recordCount, nullPercentage, drifting);
                sequence += recordCount;

                final ProcessorMetrics processorMetricsStarted = probeRunner.processor().getMetrics();
                final long cpuStarted = processCpuNanos();
                final long gcStarted = garbageCollectionMillis();
                final long wallStarted = System.nanoTime();
                final DrainResult drained = drain(runner, recordCount, null);
                final long wallNanos = System.nanoTime() - wallStarted;
                final long cpuNanos = processCpuNanos() - cpuStarted;
                final ProcessorMetrics processorMetrics = probeRunner.processor().getMetrics().subtract(processorMetricsStarted);
                final long gcMillis = garbageCollectionMillis() - gcStarted;

                measurements.add(new Measurement(iteration, drained.recordCount(), drained.flowFileCount(), drained.outputBytes(),
                        cpuNanos, processorMetrics.cpuNanos(), processorMetrics.allocatedBytes(), wallNanos, gcMillis));
            }

            return new ProbeResult(Instant.now().toString(), revision, runLabel, artifactSha256, readerMode.name(), recordCount,
                    partitionCount, iterationCount, TARGET_RECORD_BYTES, nullPercentage, drifting, MAX_POLL_RECORDS,
                    FETCH_MAX_WAIT_MILLIS, MAX_UNCOMMITTED_MILLIS, WARMUP_RECORDS, imageName,
                    System.getProperty("java.version"), ManagementFactory.getRuntimeMXBean().getInputArguments(),
                    Runtime.getRuntime().availableProcessors(), System.getProperty("os.name"), System.getProperty("os.arch"),
                    validation, List.copyOf(measurements));
        } finally {
            runner.shutdown();
        }
    }

    private static ProbeRunner createRunner(final String bootstrapServers, final String topic,
                                            final KafkaJsonReaderMode readerMode) throws Exception {
        final MeasuringConsumeKafka processor = new MeasuringConsumeKafka();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final Kafka3ConnectionService connectionService = new Kafka3ConnectionService();
        runner.addControllerService("kafka-connection", connectionService);
        runner.setProperty(connectionService, Kafka3ConnectionService.BOOTSTRAP_SERVERS, bootstrapServers);
        runner.setProperty(connectionService, Kafka3ConnectionService.SECURITY_PROTOCOL, "PLAINTEXT");
        runner.setProperty(connectionService, Kafka3ConnectionService.MAX_POLL_RECORDS, Integer.toString(MAX_POLL_RECORDS));
        runner.setProperty(connectionService, "fetch.max.wait.ms", Integer.toString(FETCH_MAX_WAIT_MILLIS));
        runner.enableControllerService(connectionService);

        final RecordReaderFactory reader = readerMode.createReader();
        runner.addControllerService("json-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(reader, "Parsing Strategy", "STANDARD");
        runner.enableControllerService(reader);

        final RecordSetWriterFactory writer = readerMode == KafkaJsonReaderMode.STREAMING
                ? new StreamingJsonRecordSetWriter()
                : new JsonRecordSetWriter();
        runner.addControllerService("json-writer", writer);
        runner.enableControllerService(writer);

        runner.setProperty("Kafka Connection Service", "kafka-connection");
        runner.setProperty("Group ID", "nifi-json-cpu-" + UUID.randomUUID());
        runner.setProperty("Topics", topic);
        runner.setProperty("auto.offset.reset", AutoOffsetReset.EARLIEST.getValue());
        runner.setProperty("Processing Strategy", ProcessingStrategy.RECORD.getValue());
        runner.setProperty("Record Reader", "json-reader");
        runner.setProperty("Record Writer", "json-writer");
        runner.setProperty("Output Strategy", OutputStrategy.USE_VALUE.getValue());
        runner.setProperty("Schema Conflict Resolution", SchemaConflictResolution.CONTINUE_WITH_MERGED_SCHEMA.getValue());
        runner.setProperty("Max Uncommitted Time", MAX_UNCOMMITTED_MILLIS + " millis");
        return new ProbeRunner(runner, processor);
    }

    private static KafkaProducer<byte[], byte[]> createProducer(final String bootstrapServers) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(256 * 1024));
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(256L * 1024 * 1024));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "600000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000");
        return new KafkaProducer<>(properties);
    }

    private static void createTopic(final String bootstrapServers, final String topic, final int partitionCount) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(List.of(new NewTopic(topic, partitionCount, (short) 1))).all().get();
        }
    }

    private static void produce(final KafkaProducer<byte[], byte[]> producer, final String topic, final int partitionCount,
                                final long firstSequence, final int recordCount, final int nullPercentage,
                                final boolean drifting) {
        final AtomicReference<Exception> failure = new AtomicReference<>();
        for (int i = 0; i < recordCount; i++) {
            final long sequence = firstSequence + i;
            final byte[] value = KafkaJsonFixture.create(sequence, TARGET_RECORD_BYTES, nullPercentage, drifting);
            producer.send(new ProducerRecord<>(topic, Math.toIntExact(sequence % partitionCount), null, value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            failure.compareAndSet(null, exception);
                        }
                    });
        }
        producer.flush();
        if (failure.get() != null) {
            throw new IllegalStateException("Kafka producer failed", failure.get());
        }
    }

    private static DrainResult drain(final TestRunner runner, final int expectedRecords,
                                     final OutputValidator outputValidator) throws Exception {
        final long deadline = System.nanoTime() + DRAIN_TIMEOUT_NANOS;
        long recordCount = 0;
        long flowFileCount = 0;
        long outputBytes = 0;

        while (recordCount < expectedRecords) {
            runner.run(1, false, false);
            final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKafka.PARSE_FAILURE);
            if (!failures.isEmpty()) {
                throw new IllegalStateException("Parse failures: " + failures.size());
            }

            final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
            for (final MockFlowFile flowFile : flowFiles) {
                recordCount += Long.parseLong(flowFile.getAttribute("record.count"));
                flowFileCount++;
                outputBytes += flowFile.getSize();
                if (outputValidator != null) {
                    outputValidator.accept(flowFile);
                }
            }
            runner.clearTransferState();

            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out after consuming " + recordCount + " of " + expectedRecords + " records");
            }
        }

        if (recordCount != expectedRecords) {
            throw new IllegalStateException("Consumed " + recordCount + " records, expected " + expectedRecords);
        }
        return new DrainResult(recordCount, flowFileCount, outputBytes);
    }

    private static long processCpuNanos() {
        return ProcessHandle.current().info().totalCpuDuration()
                .orElseThrow(() -> new IllegalStateException("Process CPU duration unavailable"))
                .toNanos();
    }

    private static long currentThreadCpuNanos() {
        if (!THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported()) {
            throw new IllegalStateException("Current thread CPU time unavailable");
        }
        if (!THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
            THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
        }
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static long currentThreadAllocatedBytes() {
        if (!(THREAD_MX_BEAN instanceof com.sun.management.ThreadMXBean allocationBean)
                || !allocationBean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Current thread allocation measurement unavailable");
        }
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        return allocationBean.getThreadAllocatedBytes(Thread.currentThread().threadId());
    }

    private static long garbageCollectionMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .filter(value -> value >= 0)
                .sum();
    }

    private static int positiveInteger(final String value, final String name) {
        final int parsed = Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }

    private static int percentage(final String value) {
        final int parsed = Integer.parseInt(value);
        if (parsed < 0 || parsed > 100) {
            throw new IllegalArgumentException("null percentage must be between 0 and 100");
        }
        return parsed;
    }

    private static String requiredSystemProperty(final String name) {
        final String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Required system property is missing: " + name);
        }
        return value;
    }

    private static String artifactSha256() throws Exception {
        final Path artifact = Path.of(KafkaJsonProcessorCpuProbe.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(artifact)) {
            throw new IllegalStateException("CPU probe must run from a benchmark JAR to record artifact identity: " + artifact);
        }

        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(artifact)) {
            final byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public record ProbeResult(String timestamp, String revision, String runLabel, String artifactSha256, String readerMode,
                              int recordsPerIteration, int partitions, int iterations, int targetRecordBytes,
                              int nullPercentage, boolean drifting, int maxPollRecords, int fetchMaxWaitMillis,
                              int maxUncommittedMillis, int warmupRecords, String kafkaImage, String javaVersion,
                              List<String> jvmArguments, int availableProcessors, String operatingSystem, String architecture,
                              ValidationSummary validation, List<Measurement> measurements) {
    }

    public record Measurement(int iteration, long records, long flowFiles, long outputBytes, long cpuNanos,
                              long processorThreadCpuNanos, long processorThreadAllocatedBytes, long wallNanos, long gcMillis) {
        public double recordsPerCpuSecond() {
            return records * 1_000_000_000D / cpuNanos;
        }

        public double cpuMicrosPerRecord() {
            return cpuNanos / 1_000D / records;
        }

        public double recordsPerProcessorThreadCpuSecond() {
            return records * 1_000_000_000D / processorThreadCpuNanos;
        }

        public double processorThreadCpuMicrosPerRecord() {
            return processorThreadCpuNanos / 1_000D / records;
        }

        public double allocatedBytesPerRecord() {
            return (double) processorThreadAllocatedBytes / records;
        }

        public double wallMicrosPerRecord() {
            return wallNanos / 1_000D / records;
        }

        public double coreEquivalent() {
            return (double) cpuNanos / wallNanos;
        }
    }

    private record DrainResult(long recordCount, long flowFileCount, long outputBytes) {
    }

    public record ValidationSummary(int records, int firmwareValues, int firmwareNulls, int firmwareMissing) {
    }

    private record ProbeRunner(TestRunner runner, MeasuringConsumeKafka processor) {
    }

    private record ProcessorMetrics(long cpuNanos, long allocatedBytes) {
        private ProcessorMetrics subtract(final ProcessorMetrics started) {
            return new ProcessorMetrics(cpuNanos - started.cpuNanos, allocatedBytes - started.allocatedBytes);
        }
    }

    private static final class MeasuringConsumeKafka extends ConsumeKafka {
        private final AtomicLong cpuNanos = new AtomicLong();
        private final AtomicLong allocatedBytes = new AtomicLong();

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            final long cpuStarted = currentThreadCpuNanos();
            final long allocatedBytesStarted = currentThreadAllocatedBytes();
            try {
                super.onTrigger(context, session);
            } finally {
                cpuNanos.addAndGet(currentThreadCpuNanos() - cpuStarted);
                allocatedBytes.addAndGet(currentThreadAllocatedBytes() - allocatedBytesStarted);
            }
        }

        private ProcessorMetrics getMetrics() {
            return new ProcessorMetrics(cpuNanos.get(), allocatedBytes.get());
        }
    }

    private static final class OutputValidator {
        private static final String FIRMWARE_VERSION = "firmwareVersion";

        private final long firstSequence;
        private final int expectedRecords;
        private final int nullPercentage;
        private final boolean drifting;
        private final BitSet observedIds;
        private int records;
        private int firmwareValues;
        private int firmwareNulls;
        private int firmwareMissing;
        private int expectedFirmwareValues;
        private int expectedFirmwareNulls;
        private int expectedFirmwareMissing;

        private OutputValidator(final long firstSequence, final int expectedRecords, final int nullPercentage,
                                final boolean drifting) {
            this.firstSequence = firstSequence;
            this.expectedRecords = expectedRecords;
            this.nullPercentage = nullPercentage;
            this.drifting = drifting;
            observedIds = new BitSet(expectedRecords);
        }

        private void accept(final MockFlowFile flowFile) throws Exception {
            final JsonNode root = OBJECT_MAPPER.readTree(flowFile.getData());
            if (!root.isArray()) {
                throw new IllegalStateException("Expected JSON array output");
            }
            final boolean widenedSchema = drifting && containsFirmwareValue(root);
            for (final JsonNode record : root) {
                validateRecord(record, widenedSchema);
            }
        }

        private boolean containsFirmwareValue(final JsonNode records) {
            for (final JsonNode record : records) {
                if (KafkaJsonFixture.hasFirmwareVersion(getSequence(record))) {
                    return true;
                }
            }
            return false;
        }

        private void validateRecord(final JsonNode record, final boolean widenedSchema) throws Exception {
            if (!(record instanceof ObjectNode objectRecord)) {
                throw new IllegalStateException("Expected JSON object record");
            }
            final long sequence = getSequence(objectRecord);
            final long relativeSequence = sequence - firstSequence;
            if (relativeSequence < 0 || relativeSequence >= expectedRecords) {
                throw new IllegalStateException("Output record id is outside the expected range: " + sequence);
            }
            final int index = Math.toIntExact(relativeSequence);
            if (observedIds.get(index)) {
                throw new IllegalStateException("Duplicate output record id: " + sequence);
            }
            observedIds.set(index);
            records++;

            final ObjectNode actualComparable = objectRecord.deepCopy();
            final JsonNode actualFirmware = actualComparable.remove(FIRMWARE_VERSION);
            validateFirmware(sequence, actualFirmware, widenedSchema);

            final ObjectNode expected = (ObjectNode) OBJECT_MAPPER.readTree(
                    KafkaJsonFixture.create(sequence, TARGET_RECORD_BYTES, nullPercentage, drifting));
            expected.remove(FIRMWARE_VERSION);
            if (!expected.equals(actualComparable)) {
                throw new IllegalStateException("Output record differs from the input fixture for id " + sequence);
            }
        }

        private long getSequence(final JsonNode record) {
            if (!(record instanceof ObjectNode objectRecord)) {
                throw new IllegalStateException("Expected JSON object record");
            }
            final JsonNode idNode = objectRecord.get("id");
            if (idNode == null || !idNode.isIntegralNumber() || !idNode.canConvertToLong()) {
                throw new IllegalStateException("Output record has an invalid id");
            }
            return idNode.longValue();
        }

        private void validateFirmware(final long sequence, final JsonNode firmware, final boolean widenedSchema) {
            if (!drifting) {
                expectedFirmwareMissing++;
                if (firmware != null) {
                    throw new IllegalStateException("Stable output contains firmwareVersion for id " + sequence);
                }
                firmwareMissing++;
            } else if (KafkaJsonFixture.hasFirmwareVersion(sequence)) {
                expectedFirmwareValues++;
                if (firmware == null || !firmware.isTextual() || !"2026.08".equals(firmware.textValue())) {
                    throw new IllegalStateException("Drifting output has an invalid firmwareVersion for id " + sequence);
                }
                firmwareValues++;
            } else if (widenedSchema) {
                expectedFirmwareNulls++;
                if (firmware == null || !firmware.isNull()) {
                    throw new IllegalStateException("Merged output is missing firmwareVersion null for id " + sequence);
                }
                firmwareNulls++;
            } else {
                expectedFirmwareMissing++;
                if (firmware != null) {
                    throw new IllegalStateException("Narrow output contains firmwareVersion for id " + sequence);
                }
                firmwareMissing++;
            }
        }

        private ValidationSummary finish() {
            if (records != expectedRecords || observedIds.cardinality() != expectedRecords) {
                throw new IllegalStateException("Validated " + records + " unique records, expected " + expectedRecords);
            }
            if (firmwareValues != expectedFirmwareValues || firmwareNulls != expectedFirmwareNulls
                    || firmwareMissing != expectedFirmwareMissing) {
                throw new IllegalStateException("Firmware validation counts differ from expected values");
            }
            return new ValidationSummary(records, firmwareValues, firmwareNulls, firmwareMissing);
        }
    }
}
