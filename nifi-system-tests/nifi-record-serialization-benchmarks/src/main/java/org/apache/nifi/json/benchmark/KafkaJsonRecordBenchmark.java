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
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.convert.RecordGroupingStrategy;
import org.apache.nifi.kafka.processors.consumer.convert.RecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Thread)
public class KafkaJsonRecordBenchmark {
    private static final int BATCH_SIZE = 100;
    private static final int MAX_TRACKED_WRITE_SCHEMAS = 64;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String STATIC_SCHEMA = """
            {
              "type":"record","name":"KafkaEvent","fields":[
                {"name":"id","type":"int"},
                {"name":"deviceId","type":"string"},
                {"name":"timestamp","type":"string"},
                {"name":"active","type":"boolean"},
                {"name":"temperature","type":"double"},
                {"name":"metrics","type":{"type":"record","name":"Metrics","fields":[
                  {"name":"cpu","type":"double"},{"name":"memory","type":"int"},{"name":"disk","type":"double"}
                ]}},
                {"name":"tags","type":{"type":"array","items":"string"}},
                {"name":"samples","type":{"type":"array","items":"int"}},
                {"name":"message","type":"string"},
                {"name":"location","type":{"type":"record","name":"Location","fields":[
                  {"name":"latitude","type":"double"},{"name":"longitude","type":"double"},{"name":"region","type":"string"}
                ]}},
                {"name":"padding","type":"string"}
              ]
            }
            """;

    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private ComponentLog logger;
    private List<ByteRecord> records;

    @Param({"2048"})
    private int targetRecordBytes;

    @Param({"LEGACY", "STREAMING"})
    private String readerMode;

    @Param({"STABLE", "DRIFTING"})
    private String schemaMode;

    @Param({"INFER"})
    private String schemaAccessMode;

    @Param({"0"})
    private int nullPercentage;

    @Param({"false"})
    private boolean allowScientificNotation;

    @Param({"ENABLED"})
    private String serializedJsonInputHandling;

    @Setup
    public void setUp() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        readerFactory = KafkaJsonReaderMode.valueOf(readerMode).createReader();
        writerFactory = "STREAMING".equals(readerMode) ? new StreamingJsonRecordSetWriter() : new JsonRecordSetWriter();
        runner.addControllerService("json-reader", readerFactory);
        switch (schemaAccessMode) {
            case "STATIC" -> {
                runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
                runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_TEXT, STATIC_SCHEMA);
            }
            case "INFER" -> runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
            default -> throw new IllegalArgumentException("Unsupported schema access mode: " + schemaAccessMode);
        }
        if (!"STABLE".equals(schemaMode) && !"DRIFTING".equals(schemaMode)) {
            throw new IllegalArgumentException("Unsupported schema mode: " + schemaMode);
        }
        runner.setProperty(readerFactory, "Parsing Strategy", "STANDARD");
        runner.enableControllerService(readerFactory);
        runner.addControllerService("json-writer", writerFactory);
        runner.setProperty(writerFactory, "Allow Scientific Notation", Boolean.toString(allowScientificNotation));
        runner.setProperty(writerFactory, "Serialized JSON Input Handling", serializedJsonInputHandling);
        runner.enableControllerService(writerFactory);
        logger = runner.getLogger();

        final List<ByteRecord> inputRecords = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            inputRecords.add(new ByteRecord("topic", i % 10, i, i, List.of(), null,
                    KafkaJsonFixture.create(i, targetRecordBytes, nullPercentage, "DRIFTING".equals(schemaMode)), 0));
        }
        records = List.copyOf(inputRecords);
        validateReaderSelection();
        validateConfiguration();
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void convertBatch(final Blackhole blackhole) {
        final WritingGrouping grouping = new WritingGrouping(writerFactory, logger, targetRecordBytes, false);

        createConverter(grouping).toFlowFiles(null, records.iterator());
        blackhole.consume(grouping.outputSize);
    }

    private RecordStreamKafkaMessageConverter createConverter(final RecordGroupingStrategy grouping) {
        return new RecordStreamKafkaMessageConverter(readerFactory, writerFactory,
                bytes -> new String(bytes, StandardCharsets.UTF_8), null, KeyEncoding.UTF8, true,
                new OffsetTracker(), logger, "broker", grouping);
    }

    private void validateConfiguration() throws Exception {
        if ("DRIFTING".equals(schemaMode)) {
            for (int partition = 0; partition < 10; partition++) {
                final int currentPartition = partition;
                final long wideRecords = records.stream()
                        .filter(record -> record.getPartition() == currentPartition && KafkaJsonFixture.hasFirmwareVersion(record.getOffset()))
                        .count();
                if (wideRecords == 0 || wideRecords == 10) {
                    throw new IllegalStateException("Every drifting partition must contain both schema shapes");
                }
            }
        }

        final WritingGrouping grouping = new WritingGrouping(writerFactory, logger, targetRecordBytes, true);
        createConverter(grouping).toFlowFiles(null, records.iterator());
        if ("STREAMING".equals(readerMode)) {
            grouping.assertStreamingRecordMaterialization("DISABLED".equals(serializedJsonInputHandling));
        }
        int recordCount = 0;
        int firmwareFields = 0;
        int firmwareValues = 0;
        for (final byte[] output : grouping.outputs) {
            final JsonNode array = OBJECT_MAPPER.readTree(output);
            if (!array.isArray()) {
                throw new IllegalStateException("Benchmark writer must produce JSON arrays");
            }
            for (final JsonNode record : array) {
                recordCount++;
                if (record.has("firmwareVersion")) {
                    firmwareFields++;
                    if (!record.get("firmwareVersion").isNull()) {
                        firmwareValues++;
                    }
                }
            }
        }
        final boolean inferredDriftingSchema = "DRIFTING".equals(schemaMode) && "INFER".equals(schemaAccessMode);
        final int expectedFirmwareValues = inferredDriftingSchema ? 10 : 0;
        final int expectedFirmwareFields = inferredDriftingSchema ? BATCH_SIZE : 0;
        if (recordCount != BATCH_SIZE || firmwareFields != expectedFirmwareFields || firmwareValues != expectedFirmwareValues) {
            throw new IllegalStateException("Benchmark output validation failed");
        }
    }

    private void validateReaderSelection() throws Exception {
        if (!"STREAMING".equals(readerMode)) {
            return;
        }

        try (RecordReader reader = ((ByteArrayRecordReaderFactory) readerFactory)
                .createRecordReaderFromBytes(Map.of(), records.getFirst().getValue(), logger)) {
            final String delegateType = getDelegateType(reader);
            final String expectedType = "INFER".equals(schemaAccessMode)
                    ? "org.apache.nifi.serialization.json.streaming.ValidatedJsonRecordReader"
                    : "org.apache.nifi.serialization.json.streaming.StreamingJsonRowRecordReader";
            if (!expectedType.equals(delegateType)) {
                throw new IllegalStateException("Unexpected Kafka benchmark reader path: " + delegateType + "; expected " + expectedType);
            }

            final RecordReader.RecordHandlingMode expectedMode = RecordReader.RecordHandlingMode.RETAINABLE;
            if (reader.getRecordHandlingMode() != expectedMode) {
                throw new IllegalStateException("Unexpected Kafka benchmark handling mode: " + reader.getRecordHandlingMode()
                        + "; expected " + expectedMode);
            }
        }
    }

    private String getDelegateType(final RecordReader reader) {
        return reader.getClass().getName();
    }

    static final class WritingGrouping implements RecordGroupingStrategy {
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private final int expectedRecordBytes;
        private final List<byte[]> outputs;
        private final Map<Integer, Group> groups = new HashMap<>();
        private int outputSize;

        private WritingGrouping(final RecordSetWriterFactory writerFactory, final ComponentLog logger,
                                final int expectedRecordBytes, final boolean retainOutputs) {
            this.writerFactory = writerFactory;
            this.logger = logger;
            this.expectedRecordBytes = expectedRecordBytes;
            this.outputs = retainOutputs ? new ArrayList<>() : null;
        }

        @Override
        public void addRecord(final ProcessSession session, final ByteRecord consumerRecord, final Record recordToWrite,
                              final RecordSchema writeSchema, final Map<String, String> attributes,
                              final Map<String, String> groupingAttributes) {
            groups.computeIfAbsent(consumerRecord.getPartition(), ignored -> new Group()).add(recordToWrite, writeSchema);
        }

        @Override
        public boolean isRecordRetentionRequired() {
            return true;
        }

        @Override
        public void finishAllGroups(final ProcessSession session) {
            for (final Group group : groups.values()) {
                final ByteArrayOutputStream output = new ByteArrayOutputStream(group.records.size() * (expectedRecordBytes + 16));
                try {
                    final RecordSchema schema = writerFactory.getSchema(Map.of(), group.mergedSchema);
                    try (RecordSetWriter writer = writerFactory.createWriter(logger, schema, output, Map.of())) {
                        writer.beginRecordSet();
                        for (final Record record : group.records) {
                            writer.write(record);
                        }
                        writer.finishRecordSet();
                    }
                } catch (final IOException | SchemaNotFoundException e) {
                    throw new IllegalStateException("Failed to write benchmark records", e);
                }
                outputSize += output.size();
                if (outputs != null) {
                    outputs.add(output.toByteArray());
                }
            }
        }

        private void assertStreamingRecordMaterialization(final boolean expectedMaterialized) throws Exception {
            for (final Group group : groups.values()) {
                for (final Record record : group.records) {
                    if (!record.getClass().getName().equals("org.apache.nifi.serialization.json.streaming.DeferredJsonRecord")) {
                        throw new IllegalStateException("Unexpected streaming benchmark record type: " + record.getClass().getName());
                    }
                    final java.lang.reflect.Method isMaterialized = record.getClass().getDeclaredMethod("isMaterialized");
                    isMaterialized.setAccessible(true);
                    if ((boolean) isMaterialized.invoke(record) != expectedMaterialized) {
                        throw new IllegalStateException(expectedMaterialized
                                ? "Streaming Kafka benchmark did not select typed JSON writing"
                                : "Streaming Kafka benchmark did not select raw JSON writing");
                    }
                }
            }
        }

        static final class Group {
            private final List<Record> records = new ArrayList<>();
            private RecordSchema mergedSchema;
            private RecordSchema firstWriteSchema;
            private List<RecordSchema> additionalWriteSchemas;
            private boolean trackDistinctWriteSchemas = true;
            private int mergeCount;

            void add(final Record record, final RecordSchema writeSchema) {
                records.add(record);
                if (records.size() == 1) {
                    firstWriteSchema = writeSchema;
                    mergedSchema = writeSchema;
                } else if (firstWriteSchema != writeSchema) {
                    if (sameSchema(firstWriteSchema, writeSchema)) {
                        if (mergedSchema == firstWriteSchema) {
                            merge(writeSchema);
                        }
                    } else if (!trackDistinctWriteSchemas) {
                        merge(writeSchema);
                    } else {
                        if (additionalWriteSchemas == null) {
                            additionalWriteSchemas = new ArrayList<>();
                        }
                        if (!alreadyMerged(writeSchema)) {
                            additionalWriteSchemas.add(writeSchema);
                            merge(writeSchema);
                            if (additionalWriteSchemas.size() == MAX_TRACKED_WRITE_SCHEMAS - 1) {
                                additionalWriteSchemas = null;
                                trackDistinctWriteSchemas = false;
                            }
                        }
                    }
                }
            }

            private void merge(final RecordSchema writeSchema) {
                mergedSchema = DataTypeUtils.merge(mergedSchema, writeSchema);
                mergeCount++;
            }

            private boolean alreadyMerged(final RecordSchema writeSchema) {
                for (final RecordSchema mergedWriteSchema : additionalWriteSchemas) {
                    if (sameSchema(mergedWriteSchema, writeSchema)) {
                        return true;
                    }
                }
                return false;
            }

            private static boolean sameSchema(final RecordSchema first, final RecordSchema second) {
                return first == second || (!first.isRecursive() && !second.isRecursive() && first.equals(second));
            }

            RecordSchema getMergedSchema() {
                return mergedSchema;
            }

            int getMergeCount() {
                return mergeCount;
            }
        }
    }

}
