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
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.io.ContentClaimInputStream;
import org.apache.nifi.controller.repository.io.TaskTerminationInputStream;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Thread)
public class StreamingJsonContentClaimBenchmark {
    private static final int TARGET_RECORD_BYTES = 2048;
    private static final int PRODUCTION_READ_LIMIT = 1_000_000;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Param({"1", "100", "600"})
    private int flowFileRecords;

    @Param({"PRODUCTION", "UNTRUSTED_SUBCLASS_REPLAY"})
    private String rewindMode;

    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;
    private ComponentLog logger;
    private byte[] input;
    private BenchmarkContentClaim contentClaim;
    private RepositoryHandler repositoryHandler;
    private ContentRepository contentRepository;
    private final CountingOutputStream output = new CountingOutputStream();

    @Setup
    public void setUp() throws Exception {
        input = createBatch(flowFileRecords);
        contentClaim = new BenchmarkContentClaim(input);
        repositoryHandler = new RepositoryHandler();
        contentRepository = (ContentRepository) Proxy.newProxyInstance(
                ContentRepository.class.getClassLoader(), new Class<?>[]{ContentRepository.class}, repositoryHandler);

        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
        readerFactory = reader;
        runner.addControllerService("streaming-json-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(reader, "Parsing Strategy", "STANDARD");
        runner.setProperty(reader, "Record Materialization Strategy", "DEFERRED");
        runner.enableControllerService(reader);

        final StreamingJsonRecordSetWriter writer = new StreamingJsonRecordSetWriter();
        writerFactory = writer;
        runner.addControllerService("streaming-json-writer", writer);
        runner.enableControllerService(writer);
        logger = runner.getLogger();

        validateFixture();
    }

    @Benchmark
    public long convertFlowFile() throws Exception {
        repositoryHandler.reset();
        output.reset();
        final long count = convert(output);
        return count ^ repositoryHandler.getReads() ^ output.getCount();
    }

    private long convert(final OutputStream destination) throws Exception {
        long records = 0;
        try (InputStream contentStream = createContentStream();
             RecordReader reader = readerFactory.createRecordReader(Map.of(), contentStream, input.length, logger)) {
            final RecordSchema writeSchema = writerFactory.getSchema(Map.of(), reader.getSchema());
            try (RecordSetWriter writer = writerFactory.createWriter(logger, writeSchema, destination, Map.of())) {
                writer.beginRecordSet();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    writer.write(record);
                    records++;
                }
                writer.finishRecordSet();
            }
        }
        return records;
    }

    private InputStream createContentStream() {
        final ContentClaimInputStream contentStream = "UNTRUSTED_SUBCLASS_REPLAY".equals(rewindMode)
                ? new UntrustedContentClaimInputStream(contentRepository, contentClaim)
                : new ContentClaimInputStream(contentRepository, contentClaim, 0, new NopPerformanceTracker());
        return new TaskTerminationInputStream(contentStream, () -> false, null);
    }

    private void validateFixture() throws Exception {
        validateReaderPath();
        repositoryHandler.reset();
        final ByteArrayOutputStream validationOutput = new ByteArrayOutputStream(input.length + 2);
        final long records = convert(validationOutput);
        if (records != flowFileRecords) {
            throw new IllegalStateException("Unexpected record count: " + records + "; expected " + flowFileRecords);
        }

        final JsonNode outputRecords = OBJECT_MAPPER.readTree(validationOutput.toByteArray());
        if (!outputRecords.isArray() || outputRecords.size() != flowFileRecords) {
            throw new IllegalStateException("Content Claim benchmark output validation failed");
        }

        final long expectedRepositoryReads = "PRODUCTION".equals(rewindMode) && input.length > PRODUCTION_READ_LIMIT ? 2 : 1;
        if (repositoryHandler.getReads() != expectedRepositoryReads) {
            throw new IllegalStateException("Unexpected repository reads: " + repositoryHandler.getReads()
                    + "; expected " + expectedRepositoryReads + "; input bytes=" + input.length);
        }
    }

    private void validateReaderPath() throws Exception {
        repositoryHandler.reset();
        try (InputStream contentStream = createContentStream();
             RecordReader reader = readerFactory.createRecordReader(Map.of(), contentStream, input.length, logger)) {
            final String delegateType = reader.getClass().getName();
            if (!delegateType.equals("org.apache.nifi.serialization.json.streaming.ValidatedInputStreamRecordReader")) {
                throw new IllegalStateException("Unexpected benchmark reader path: " + delegateType);
            }
            if (reader.getRecordHandlingMode() != RecordReader.RecordHandlingMode.RETAINABLE) {
                throw new IllegalStateException("Unexpected benchmark record handling mode: " + reader.getRecordHandlingMode());
            }
        }
    }

    private static byte[] createBatch(final int records) {
        final StringBuilder builder = new StringBuilder(records * (TARGET_RECORD_BYTES + 1));
        builder.append('[');
        for (int i = 0; i < records; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(new String(KafkaJsonFixture.create(i, TARGET_RECORD_BYTES, 0, false), StandardCharsets.UTF_8));
        }
        return builder.append(']').toString().getBytes(StandardCharsets.UTF_8);
    }

    private final class RepositoryHandler implements InvocationHandler {
        private long reads;

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] arguments) {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "toString" -> StreamingJsonContentClaimBenchmark.class.getSimpleName() + "Repository";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            }
            if (method.getName().equals("read") && method.getParameterTypes()[0] == ContentClaim.class) {
                reads++;
                return new ByteArrayInputStream(((BenchmarkContentClaim) arguments[0]).content());
            }
            throw new UnsupportedOperationException(method.toGenericString());
        }

        private void reset() {
            reads = 0;
        }

        private long getReads() {
            return reads;
        }
    }

    private record BenchmarkContentClaim(byte[] content) implements ContentClaim {
        @Override
        public ResourceClaim getResourceClaim() {
            return null;
        }

        @Override
        public long getOffset() {
            return 0;
        }

        @Override
        public long getLength() {
            return content.length;
        }

        @Override
        public int compareTo(final ContentClaim other) {
            return this == other ? 0 : Integer.compare(System.identityHashCode(this), System.identityHashCode(other));
        }
    }

    private static final class UntrustedContentClaimInputStream extends ContentClaimInputStream {
        private UntrustedContentClaimInputStream(final ContentRepository contentRepository, final ContentClaim contentClaim) {
            super(contentRepository, contentClaim, 0, new NopPerformanceTracker());
        }
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
}
