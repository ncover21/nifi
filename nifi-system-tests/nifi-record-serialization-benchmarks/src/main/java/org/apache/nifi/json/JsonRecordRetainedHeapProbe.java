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
package org.apache.nifi.json;

import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.openjdk.jol.info.GraphLayout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.ref.Reference;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JsonRecordRetainedHeapProbe {
    private final TestRunner runner;
    private final ServicePair legacy;
    private final ServicePair streaming;

    private JsonRecordRetainedHeapProbe() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        legacy = configure("legacy", new JsonTreeReader(), new JsonRecordSetWriter());
        streaming = configure("streaming", new StreamingJsonRecordReader(), new StreamingJsonRecordSetWriter());
    }

    public static void main(final String[] arguments) throws Exception {
        final int[] recordCounts = parseRecordCounts(arguments);
        final JsonRecordRetainedHeapProbe probe = new JsonRecordRetainedHeapProbe();
        try {
            System.out.println("mode,scenario,records,wireBytes,preWriteBytes,postWriteBytes,stagedGraphBytes,"
                    + "preWriteBytesPerRecord,postWriteBytesPerRecord,postWriteToWire,serializedReferences,uniqueSourceArrays");
            for (final int recordCount : recordCounts) {
                for (final Scenario scenario : Scenario.values()) {
                    for (final ReaderMode mode : ReaderMode.values()) {
                        if (isSelected(mode, scenario)) {
                            probe.run(mode, scenario, recordCount);
                        }
                    }
                }
            }
        } finally {
            probe.runner.shutdown();
        }
    }

    private ServicePair configure(final String id, final RecordReaderFactory reader, final RecordSetWriterFactory writer) throws Exception {
        runner.addControllerService(id + "-reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(reader, "Parsing Strategy", "STANDARD");
        runner.enableControllerService(reader);
        runner.addControllerService(id + "-writer", writer);
        runner.enableControllerService(writer);
        return new ServicePair(reader, writer);
    }

    private void run(final ReaderMode mode, final Scenario scenario, final int recordCount) throws Exception {
        final List<byte[]> payloads = createPayloads(scenario, recordCount);
        final List<Record> records = new ArrayList<>(recordCount);
        for (final byte[] payload : payloads) {
            records.addAll(read(mode, payload));
        }
        if (records.size() != recordCount) {
            throw new IllegalStateException("Expected %d records but read %d".formatted(recordCount, records.size()));
        }

        final ServicePair services = services(mode);
        final long serviceBytes = GraphLayout.parseInstance(services.reader(), services.writer()).totalSize();
        final long preWriteBytes = GraphLayout.parseInstance(services.reader(), services.writer(), records).totalSize() - serviceBytes;
        final long stagedGraphBytes = GraphLayout.parseInstance(services.reader(), services.writer(), records, payloads).totalSize() - serviceBytes;
        final SourceReferences sourceReferences = inspectSourceReferences(mode, records, payloads);

        write(services.writer(), records);
        if (mode == ReaderMode.STREAMING) {
            assertRecordsRemainDeferred(records);
        }
        final long postWriteBytes = GraphLayout.parseInstance(services.reader(), services.writer(), records).totalSize() - serviceBytes;
        final long wireBytes = payloads.stream().mapToLong(payload -> payload.length).sum();

        System.out.printf("%s,%s,%d,%d,%d,%d,%d,%.2f,%.2f,%.4f,%d,%d%n",
                mode, scenario, recordCount, wireBytes, preWriteBytes, postWriteBytes, stagedGraphBytes,
                (double) preWriteBytes / recordCount, (double) postWriteBytes / recordCount,
                (double) postWriteBytes / wireBytes, sourceReferences.references(), sourceReferences.uniqueSources());

        final long holdMillis = Long.getLong("probe.holdMillis", 0L);
        if (holdMillis > 0) {
            System.gc();
            Thread.sleep(100);
            System.out.printf("Holding retained graph for %d ms in process %d%n", holdMillis, ProcessHandle.current().pid());
            Thread.sleep(holdMillis);
        }
        Reference.reachabilityFence(records);
        Reference.reachabilityFence(payloads);
    }

    private List<Record> read(final ReaderMode mode, final byte[] payload) throws Exception {
        final RecordReaderFactory readerFactory = services(mode).reader();
        final List<Record> records = new ArrayList<>();
        try (RecordReader reader = mode == ReaderMode.STREAMING
                ? ((ByteArrayRecordReaderFactory) readerFactory).createRecordReaderFromBytes(Map.of(), payload, runner.getLogger())
                : readerFactory.createRecordReader(Map.of(), new ByteArrayInputStream(payload), payload.length, runner.getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private void write(final RecordSetWriterFactory writerFactory, final List<Record> records) throws Exception {
        RecordSchema mergedSchema = null;
        for (final Record record : records) {
            mergedSchema = DataTypeUtils.merge(mergedSchema, record.getSchema());
        }
        final RecordSchema writeSchema = writerFactory.getSchema(Map.of(), mergedSchema);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
             RecordSetWriter writer = writerFactory.createWriter(runner.getLogger(), writeSchema, output, Map.of())) {
            writer.write(RecordSet.of(writeSchema, records.toArray(Record[]::new)));
        }
    }

    private SourceReferences inspectSourceReferences(final ReaderMode mode, final List<Record> records, final List<byte[]> payloads) throws Exception {
        final Set<byte[]> uniqueSources = Collections.newSetFromMap(new IdentityHashMap<>());
        int references = 0;
        for (final Record record : records) {
            final Object serialized = record.getSerializedForm().map(form -> form.getSerialized()).orElse(null);
            if (serialized == null || !serialized.getClass().getName().equals("org.apache.nifi.serialization.json.streaming.Utf8JsonValue")) {
                continue;
            }
            final Method isBackedBy = serialized.getClass().getDeclaredMethod("isBackedBy", byte[].class);
            isBackedBy.setAccessible(true);
            references++;
            for (final byte[] payload : payloads) {
                if ((boolean) isBackedBy.invoke(serialized, payload)) {
                    uniqueSources.add(payload);
                    break;
                }
            }
        }
        if (mode == ReaderMode.STREAMING && (references != records.size() || uniqueSources.size() != payloads.size())) {
            throw new IllegalStateException("Expected every streaming record to reference its source payload");
        }
        return new SourceReferences(references, uniqueSources.size());
    }

    private void assertRecordsRemainDeferred(final List<Record> records) throws Exception {
        for (final Record record : records) {
            if (!record.getClass().getName().equals("org.apache.nifi.serialization.json.streaming.DeferredJsonRecord")) {
                throw new IllegalStateException("Unexpected streaming record type: " + record.getClass().getName());
            }
            final Method isMaterialized = record.getClass().getDeclaredMethod("isMaterialized");
            isMaterialized.setAccessible(true);
            if ((boolean) isMaterialized.invoke(record)) {
                throw new IllegalStateException("Streaming writer materialized an eligible retained record");
            }
        }
    }

    private ServicePair services(final ReaderMode mode) {
        return mode == ReaderMode.LEGACY ? legacy : streaming;
    }

    private List<byte[]> createPayloads(final Scenario scenario, final int recordCount) {
        if (scenario == Scenario.ONE_ARRAY_MESSAGE) {
            final StringBuilder json = new StringBuilder(recordCount * 512);
            json.append('[');
            for (int i = 0; i < recordCount; i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append(createJson(i));
            }
            return List.of(json.append(']').toString().getBytes(StandardCharsets.UTF_8));
        }

        final List<byte[]> payloads = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            payloads.add(createJson(i).getBytes(StandardCharsets.UTF_8));
        }
        return payloads;
    }

    private String createJson(final int sequence) {
        return """
                {"id":%d,"deviceId":"device-%04d","timestamp":"2026-08-27T10:15:30.123Z",\
                "active":true,"temperature":72.125,"metrics":{"cpu":0.42,"memory":7340032,"disk":0.81},\
                "tags":["production","west","sensor"],"samples":[12,13,15,18,21,34],\
                "message":"The quick brown fox jumps over the lazy dog while carrying a representative Kafka event payload",\
                "location":{"latitude":37.7749,"longitude":-122.4194,"region":"us-west"}}
                """.formatted(sequence, sequence).strip();
    }

    private static int[] parseRecordCounts(final String[] arguments) {
        if (arguments.length == 0) {
            return new int[]{1, 100, 1000};
        }
        final int[] recordCounts = new int[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            recordCounts[i] = Integer.parseInt(arguments[i]);
            if (recordCounts[i] < 1) {
                throw new IllegalArgumentException("Record counts must be positive");
            }
        }
        return recordCounts;
    }

    private static boolean isSelected(final ReaderMode mode, final Scenario scenario) {
        final String selectedMode = System.getProperty("probe.mode");
        final String selectedScenario = System.getProperty("probe.scenario");
        return (selectedMode == null || mode.name().equalsIgnoreCase(selectedMode))
                && (selectedScenario == null || scenario.name().equalsIgnoreCase(selectedScenario));
    }

    private enum ReaderMode {
        LEGACY,
        STREAMING
    }

    private enum Scenario {
        MANY_MESSAGES,
        ONE_ARRAY_MESSAGE
    }

    private record ServicePair(RecordReaderFactory reader, RecordSetWriterFactory writer) {
    }

    private record SourceReferences(int references, int uniqueSources) {
    }
}
