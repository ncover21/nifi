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
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSchemaCacheService;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class StreamingJsonServiceInputStreamBenchmark {
    private static final int BATCH_SIZE = 100;
    private static final String CACHE_IDENTIFIER = "benchmark-schema";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Param({"INPUT_STREAM", "FINITE_MARK_STREAM", "DIRECT_BYTES", "LEGACY_INPUT_STREAM"})
    private String entryPoint;

    @Param({"INFER", "STATIC", "CACHE_HIT"})
    private String schemaAccess;

    @Param({"ARRAY", "JSONL"})
    private String framing;

    @Param({"256", "2048", "16384"})
    private int targetRecordBytes;

    @Param({"PASS_THROUGH", "READ_ONE_FIELD", "MUTATE_ONE_FIELD"})
    private String recordOperation;

    @Param({"DEFERRED"})
    private String materializationStrategy;

    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private ComponentLog logger;
    private Map<String, String> variables;
    private byte[] singleRecord;
    private byte[] recordBatch;
    private boolean accessRecord;
    private boolean mutateRecord;

    @Setup
    public void setUp() throws Exception {
        switch (recordOperation) {
            case "PASS_THROUGH" -> {
                accessRecord = false;
                mutateRecord = false;
            }
            case "READ_ONE_FIELD" -> {
                accessRecord = true;
                mutateRecord = false;
            }
            case "MUTATE_ONE_FIELD" -> {
                accessRecord = true;
                mutateRecord = true;
            }
            default -> throw new IllegalArgumentException("Unsupported record operation: " + recordOperation);
        }
        singleRecord = KafkaJsonFixture.create(1, targetRecordBytes, 0, false);
        recordBatch = createBatch(framing, targetRecordBytes);

        final RecordSchema schema = inferSchema(singleRecord);
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        if ("LEGACY_INPUT_STREAM".equals(entryPoint)) {
            final JsonTreeReader reader = new JsonTreeReader();
            readerFactory = reader;
            runner.addControllerService("json-tree-reader", reader);
            configureSchemaAccess(runner, reader, schema);
            runner.setProperty(reader, "Parsing Strategy", "STANDARD");
            runner.enableControllerService(reader);

            final JsonRecordSetWriter writer = new JsonRecordSetWriter();
            writerFactory = writer;
            runner.addControllerService("json-record-set-writer", writer);
            runner.enableControllerService(writer);
        } else {
            final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
            readerFactory = reader;
            runner.addControllerService("streaming-json-reader", reader);
            configureSchemaAccess(runner, reader, schema);
            runner.setProperty(reader, "Parsing Strategy", "STANDARD");
            runner.setProperty(reader, "Record Materialization Strategy", materializationStrategy);
            runner.enableControllerService(reader);

            final StreamingJsonRecordSetWriter writer = new StreamingJsonRecordSetWriter();
            writerFactory = writer;
            runner.addControllerService("streaming-json-writer", writer);
            runner.enableControllerService(writer);
        }
        logger = runner.getLogger();

        assertFiniteMarkStreamReplayClassification();
        validate(singleRecord, 1, 1);
        validate(recordBatch, BATCH_SIZE, 0);
    }

    @Benchmark
    public void convertSingleRecord(final TimedOutput timedOutput, final Blackhole blackhole) throws Exception {
        blackhole.consume(convert(singleRecord, timedOutput.output));
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public void convertRecordBatch(final TimedOutput timedOutput, final Blackhole blackhole) throws Exception {
        blackhole.consume(convert(recordBatch, timedOutput.output));
    }

    private long convert(final byte[] input, final CountingOutputStream output) throws Exception {
        output.reset();
        long checksum = 0;
        try (RecordReader reader = createReader(input)) {
            final RecordSchema writeSchema = writerFactory.getSchema(variables, reader.getSchema());
            try (RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, output, variables)) {
                writer.beginRecordSet();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    checksum += applyRecordOperation(record);
                    writer.write(record);
                }
                writer.finishRecordSet();
            }
        }
        return output.getCount() ^ checksum;
    }

    private long applyRecordOperation(final Record record) {
        if (!accessRecord) {
            return 0;
        }
        final long value = record.getAsLong("id") + (mutateRecord ? 1 : 0);
        if (mutateRecord) {
            record.setValue("id", value);
        }
        return value;
    }

    private RecordReader createReader(final byte[] input) throws Exception {
        if ("DIRECT_BYTES".equals(entryPoint)) {
            return ((ByteArrayRecordReaderFactory) readerFactory).createRecordReaderFromBytes(variables, input, logger);
        }
        final InputStream inputStream = "FINITE_MARK_STREAM".equals(entryPoint)
                ? new FiniteMarkInputStream(input) : new ByteArrayInputStream(input);
        return readerFactory.createRecordReader(variables, inputStream, input.length, logger);
    }

    private void configureSchemaAccess(final TestRunner runner, final ControllerService reader,
                                       final RecordSchema schema) throws Exception {
        variables = Map.of();
        switch (schemaAccess) {
            case "INFER" -> runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
            case "STATIC" -> {
                runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "schema-text-property");
                runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, AvroTypeUtil.extractAvroSchema(schema).toString());
            }
            case "CACHE_HIT" -> {
                final BenchmarkSchemaCache cache = new BenchmarkSchemaCache(schema);
                runner.addControllerService("benchmark-schema-cache", cache);
                runner.enableControllerService(cache);
                runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
                runner.setProperty(reader, "Schema Inference Cache", "benchmark-schema-cache");
                variables = Map.of(RecordSchemaCacheService.CACHE_IDENTIFIER_ATTRIBUTE, CACHE_IDENTIFIER);
            }
            default -> throw new IllegalArgumentException("Unsupported schema access mode: " + schemaAccess);
        }
    }

    private RecordSchema inferSchema(final byte[] input) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
        runner.addControllerService("schema-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(reader, "Parsing Strategy", "STANDARD");
        runner.enableControllerService(reader);
        try (RecordReader recordReader = reader.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            return recordReader.getSchema();
        }
    }

    private byte[] createBatch(final String batchFraming, final int recordBytes) {
        final StringBuilder builder = new StringBuilder(BATCH_SIZE * (recordBytes + 1));
        if ("ARRAY".equals(batchFraming)) {
            builder.append('[');
        }
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i > 0) {
                builder.append("ARRAY".equals(batchFraming) ? ',' : '\n');
            }
            builder.append(new String(KafkaJsonFixture.create(i, recordBytes, 0, false), StandardCharsets.UTF_8));
        }
        if ("ARRAY".equals(batchFraming)) {
            builder.append(']');
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void validate(final byte[] input, final int expectedRecords, final long firstId) throws Exception {
        final ByteArrayOutputStream output = new ByteArrayOutputStream(input.length + 2);
        try (RecordReader reader = createReader(input)) {
            assertReaderPath(reader);
            final RecordSchema writeSchema = writerFactory.getSchema(variables, reader.getSchema());
            try (RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, output, variables)) {
                writer.beginRecordSet();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    applyRecordOperation(record);
                    writer.write(record);
                    assertMaterializationStrategy(record);
                }
                writer.finishRecordSet();
            }
        }

        final JsonNode records = OBJECT_MAPPER.readTree(output.toByteArray());
        if (!records.isArray() || records.size() != expectedRecords) {
            throw new IllegalStateException("Benchmark output validation failed");
        }
        for (int i = 0; i < expectedRecords; i++) {
            final long expectedId = firstId + i + (mutateRecord ? 1 : 0);
            if (records.get(i).path("id").asLong(Long.MIN_VALUE) != expectedId) {
                throw new IllegalStateException("Benchmark transformed value validation failed");
            }
        }
    }

    private void assertFiniteMarkStreamReplayClassification() throws Exception {
        if (!"FINITE_MARK_STREAM".equals(entryPoint)) {
            return;
        }

        final FiniteMarkInputStream input = new FiniteMarkInputStream(singleRecord);
        try (RecordReader reader = readerFactory.createRecordReader(variables, input, singleRecord.length, logger)) {
            assertReaderPath(reader);
        }
        if (input.marks != 0 || input.resets != 0) {
            throw new IllegalStateException("Finite-mark stream bypassed bounded replay: marks=" + input.marks
                    + ", resets=" + input.resets);
        }
    }

    private void assertMaterializationStrategy(final Record record) throws Exception {
        if (record == null || "LEGACY_INPUT_STREAM".equals(entryPoint)
                || "EAGER".equals(materializationStrategy)
                || !record.getClass().getName().equals("org.apache.nifi.serialization.json.streaming.DeferredJsonRecord")) {
            return;
        }

        final java.lang.reflect.Method isMaterialized = record.getClass().getDeclaredMethod("isMaterialized");
        isMaterialized.setAccessible(true);
        final boolean materialized = (boolean) isMaterialized.invoke(record);
        final boolean expectedMaterialized = accessRecord;
        if (materialized != expectedMaterialized) {
            throw new IllegalStateException("Unexpected benchmark record materialization state: " + materialized
                    + "; expected " + expectedMaterialized);
        }
    }

    private void assertReaderPath(final RecordReader reader) throws Exception {
        final String delegateType = getDelegateType(reader);
        final String expectedType;
        if ("LEGACY_INPUT_STREAM".equals(entryPoint)) {
            expectedType = "org.apache.nifi.json.JsonTreeRowRecordReader";
        } else if ("DEFERRED".equals(materializationStrategy) && "DIRECT_BYTES".equals(entryPoint) && "INFER".equals(schemaAccess)) {
            expectedType = "org.apache.nifi.serialization.json.streaming.ValidatedJsonRecordReader";
        } else if ("DEFERRED".equals(materializationStrategy)
                && ("INPUT_STREAM".equals(entryPoint) || "FINITE_MARK_STREAM".equals(entryPoint))
                && "INFER".equals(schemaAccess)) {
            expectedType = "org.apache.nifi.serialization.json.streaming.ValidatedInputStreamRecordReader";
        } else {
            expectedType = "org.apache.nifi.serialization.json.streaming.StreamingJsonRowRecordReader";
        }
        if (!expectedType.equals(delegateType)) {
            throw new IllegalStateException("Unexpected benchmark reader path: " + delegateType + "; expected " + expectedType);
        }

        if (!"LEGACY_INPUT_STREAM".equals(entryPoint)) {
            final RecordReader.RecordHandlingMode expectedMode = RecordReader.RecordHandlingMode.RETAINABLE;
            if (reader.getRecordHandlingMode() != expectedMode) {
                throw new IllegalStateException("Unexpected benchmark record handling mode: " + reader.getRecordHandlingMode()
                        + "; expected " + expectedMode);
            }
        }
    }

    private String getDelegateType(final RecordReader reader) throws Exception {
        Object delegate = reader;
        while (true) {
            final String className = delegate.getClass().getName();
            if (className.equals("org.apache.nifi.serialization.json.streaming.ReplayOwningRecordReader")) {
                final java.lang.reflect.Field delegateField = delegate.getClass().getDeclaredField("delegate");
                delegateField.setAccessible(true);
                delegate = delegateField.get(delegate);
            } else {
                return className;
            }
        }
    }

    @State(Scope.Thread)
    public static class TimedOutput {
        private final CountingOutputStream output = new CountingOutputStream();
    }

    private static final class CountingOutputStream extends OutputStream {
        private long count;

        @Override
        public void write(final int value) {
            count++;
        }

        @Override
        public void write(final byte[] buffer, final int offset, final int length) {
            count += length;
        }

        private void reset() {
            count = 0;
        }

        private long getCount() {
            return count;
        }
    }

    private static final class FiniteMarkInputStream extends ByteArrayInputStream {
        private int marks;
        private int resets;

        private FiniteMarkInputStream(final byte[] input) {
            super(input);
        }

        @Override
        public synchronized void mark(final int readLimit) {
            marks++;
            super.mark(readLimit);
        }

        @Override
        public synchronized void reset() {
            resets++;
            super.reset();
        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }

    private static final class BenchmarkSchemaCache extends AbstractControllerService implements RecordSchemaCacheService {
        private final RecordSchema schema;

        private BenchmarkSchemaCache(final RecordSchema schema) {
            this.schema = schema;
        }

        @Override
        public String cacheSchema(final RecordSchema schema) {
            return CACHE_IDENTIFIER;
        }

        @Override
        public Optional<RecordSchema> getSchema(final String schemaIdentifier) {
            return CACHE_IDENTIFIER.equals(schemaIdentifier) ? Optional.of(schema) : Optional.empty();
        }
    }
}
