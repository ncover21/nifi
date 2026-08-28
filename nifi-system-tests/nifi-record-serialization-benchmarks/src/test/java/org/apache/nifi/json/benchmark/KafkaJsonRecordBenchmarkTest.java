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

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaJsonRecordBenchmarkTest {

    @Test
    void testSchemaTrackingMatchesSequentialMergeAcrossBound() {
        final List<RecordSchema> schemas = new ArrayList<>();
        for (int i = 0; i < 70; i++) {
            schemas.add(schema("field" + i));
            if (i % 7 == 0) {
                schemas.add(schema("field" + i));
            }
        }

        RecordSchema expected = null;
        final KafkaJsonRecordBenchmark.WritingGrouping.Group group =
                new KafkaJsonRecordBenchmark.WritingGrouping.Group();
        for (final RecordSchema schema : schemas) {
            expected = DataTypeUtils.merge(expected, schema);
            group.add(new MapRecord(schema, Map.of()), schema);
        }

        assertEquals(expected, group.getMergedSchema());
    }

    @Test
    void testStructurallyEqualSchemasMergeOnce() {
        final RecordSchema first = schema("field");
        final RecordSchema second = schema("field");
        final KafkaJsonRecordBenchmark.WritingGrouping.Group group =
                new KafkaJsonRecordBenchmark.WritingGrouping.Group();

        group.add(new MapRecord(first, Map.of()), first);
        group.add(new MapRecord(second, Map.of()), second);
        group.add(new MapRecord(first, Map.of()), first);

        final RecordSchema expected = DataTypeUtils.merge(first, second);
        assertEquals(expected, group.getMergedSchema());
        assertEquals(expected.getSchemaName(), group.getMergedSchema().getSchemaName());
        assertEquals(1, group.getMergeCount());
        assertTrue(group.getMergedSchema().getSchemaName().isEmpty());
    }

    @Test
    void testReaderModesUseIntendedHandling() throws Exception {
        assertHandlingMode(KafkaJsonReaderMode.LEGACY, RecordReader.RecordHandlingMode.RETAINABLE);
        assertHandlingMode(KafkaJsonReaderMode.STREAMING, RecordReader.RecordHandlingMode.RETAINABLE);
        assertInstanceOf(JsonTreeReader.class, KafkaJsonReaderMode.LEGACY.createReader());
        assertInstanceOf(StreamingJsonRecordReader.class, KafkaJsonReaderMode.STREAMING.createReader());
    }

    @Test
    void testStreamingTypedWriterControlValidatesMaterialization() throws Exception {
        final KafkaJsonRecordBenchmark benchmark = new KafkaJsonRecordBenchmark();
        setField(benchmark, "targetRecordBytes", 2048);
        setField(benchmark, "readerMode", "STREAMING");
        setField(benchmark, "schemaMode", "STABLE");
        setField(benchmark, "schemaAccessMode", "INFER");
        setField(benchmark, "nullPercentage", 0);
        setField(benchmark, "allowScientificNotation", false);
        setField(benchmark, "serializedJsonInputHandling", "DISABLED");

        benchmark.setUp();
    }

    private static void assertHandlingMode(final KafkaJsonReaderMode mode, final RecordReader.RecordHandlingMode expected) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final RecordReaderFactory readerFactory = mode.createReader();
        runner.addControllerService("json-reader", readerFactory);
        runner.setProperty(readerFactory, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(readerFactory, "Parsing Strategy", "STANDARD");
        runner.enableControllerService(readerFactory);

        final byte[] input = "{\"value\":\"test\"}".getBytes(StandardCharsets.UTF_8);
        final RecordReader reader = readerFactory instanceof final ByteArrayRecordReaderFactory byteArrayFactory
                ? byteArrayFactory.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())
                : readerFactory.createRecordReader(Map.of(), new java.io.ByteArrayInputStream(input), input.length, runner.getLogger());
        try (reader) {
            assertEquals(expected, reader.getRecordHandlingMode());
            assertEquals("test", reader.nextRecord().getValue("value"));
        }
    }

    private static RecordSchema schema(final String fieldName) {
        return new SimpleRecordSchema(List.of(new RecordField(fieldName, RecordFieldType.STRING.getDataType())));
    }

    private static void setField(final KafkaJsonRecordBenchmark benchmark, final String fieldName, final Object value) throws Exception {
        final java.lang.reflect.Field field = KafkaJsonRecordBenchmark.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(benchmark, value);
    }
}
