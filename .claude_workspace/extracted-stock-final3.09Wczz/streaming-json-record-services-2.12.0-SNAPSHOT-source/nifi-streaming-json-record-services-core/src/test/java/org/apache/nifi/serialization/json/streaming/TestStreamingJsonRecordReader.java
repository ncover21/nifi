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
package org.apache.nifi.serialization.json.streaming;

import org.apache.nifi.NullSuppression;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSchemaCacheService;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.serialization.json.streaming.SchemaInferenceUtil.OBSOLETE_SCHEMA_CACHE;
import static org.apache.nifi.serialization.json.streaming.SchemaInferenceUtil.SCHEMA_CACHE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStreamingJsonRecordReader {

    @Test
    void testCreateRecordReaderFromBytesUsesValidatedReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertInstanceOf(ValidatedJsonRecordReader.class, reader);
            final Record record = reader.nextRecord();
            final DeferredJsonRecord deferredRecord = assertInstanceOf(DeferredJsonRecord.class, record);
            assertNotNull(record);
            assertInstanceOf(Utf8JsonValue.class, record.getSerializedForm().orElseThrow().getSerialized());
            assertFalse(deferredRecord.isMaterialized());
            assertTrue(record.isTypeChecked());
            assertEquals(42, record.getValue("number"));
            assertTrue(deferredRecord.isMaterialized());
            assertFalse(deferredRecord.hasPendingState());
            assertTrue(record.isTypeChecked());
            assertInstanceOf(Utf8JsonValue.class, record.getSerializedForm().orElseThrow().getSerialized());
            record.setValue("number", 43);
            assertEquals(43, record.getValue("number"));
            assertFalse(record.getSerializedForm().isPresent());
        }
    }

    @Test
    void testNonUtf8ByteInputWritesNormalizedUtf8() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);

        for (final java.nio.charset.Charset charset : List.of(StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE,
                java.nio.charset.Charset.forName("UTF-32BE"), java.nio.charset.Charset.forName("UTF-32LE"))) {
            for (final String prefix : List.of("", "\uFEFF")) {
                final byte[] input = (prefix + "{\"number\":42}").getBytes(charset);
                try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
                    assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
                    final Record record = reader.nextRecord();
                    final ByteArrayOutputStream output = new ByteArrayOutputStream();
                    try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(runner.getLogger(), record.getSchema(),
                            new SchemaNameAsAttribute(), output, false, NullSuppression.NEVER_SUPPRESS,
                            OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                            RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
                        writer.write(RecordSet.of(record.getSchema(), record));
                    }

                    assertEquals("[{\"number\":42}]", output.toString(StandardCharsets.UTF_8));
                    assertNull(reader.nextRecord());
                }
            }
        }
    }

    @Test
    void testNonUtf8InputStreamUsesNormalizedEagerReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);

        for (final java.nio.charset.Charset charset : List.of(StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE,
                java.nio.charset.Charset.forName("UTF-32BE"), java.nio.charset.Charset.forName("UTF-32LE"))) {
            for (final String prefix : List.of("", "\uFEFF")) {
                final byte[] input = (prefix + "{\"number\":42}").getBytes(charset);
                try (RecordReader reader = service.createRecordReader(
                        Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
                    assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
                    assertEquals(42, reader.nextRecord().getValue("number"));
                    assertNull(reader.nextRecord());
                }
            }
        }
    }

    @Test
    void testValidatedRecordWritesSerializedBytesWithoutMaterialization() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (StreamingJsonWriteResult writer = new StreamingJsonWriteResult(runner.getLogger(), record.getSchema(), new SchemaNameAsAttribute(), output, false,
                    NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                    RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "application/json", true, true)) {
                writer.write(RecordSet.of(record.getSchema(), record));
            }

            assertEquals("[{\"number\":42}]", output.toString(StandardCharsets.UTF_8));
            assertFalse(record.isMaterialized());
        }
    }

    @Test
    void testInputStreamReaderAndWriterPassThroughWithoutMaterialization() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService readerService = enableReader(runner, ParsingStrategy.STANDARD);
        final StreamingJsonRecordSetWriter writerService = new StreamingJsonRecordSetWriter();
        runner.addControllerService("streaming-writer", writerService);
        runner.enableControllerService(writerService);
        final byte[] input = " { \"number\" : 42 } ".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = readerService.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            final RecordSchema writeSchema = writerService.getSchema(Map.of(), reader.getSchema());
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (RecordSetWriter writer = writerService.createWriter(runner.getLogger(), writeSchema, output, Map.of())) {
                writer.write(RecordSet.of(writeSchema, record));
            }

            assertEquals("[{ \"number\" : 42 }]", output.toString(StandardCharsets.UTF_8));
            assertFalse(record.isMaterialized());
        }
    }

    @Test
    void testInputStreamTransformationForcesTypedWriterPath() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService readerService = enableReader(runner, ParsingStrategy.STANDARD);
        final StreamingJsonRecordSetWriter writerService = new StreamingJsonRecordSetWriter();
        runner.addControllerService("streaming-writer", writerService);
        runner.enableControllerService(writerService);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = readerService.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            record.setValue("number", record.getAsLong("number") + 1);
            final RecordSchema writeSchema = writerService.getSchema(Map.of(), reader.getSchema());
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (RecordSetWriter writer = writerService.createWriter(runner.getLogger(), writeSchema, output, Map.of())) {
                writer.write(RecordSet.of(writeSchema, record));
            }

            assertEquals("[{\"number\":43}]", output.toString(StandardCharsets.UTF_8));
            assertTrue(record.isMaterialized());
            assertFalse(record.getSerializedForm().isPresent());
        }
    }

    @Test
    void testEagerMaterializationStrategyUsesStreamingReaders() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.RECORD_MATERIALIZATION_STRATEGY,
                RecordMaterializationStrategy.EAGER.getValue());
        runner.enableControllerService(service);
        final byte[] input = "[{\"number\":42},{\"number\":43}]".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            final Record record = reader.nextRecord();
            assertEquals(42, record.getValue("number"));
            assertFalse(record.getSerializedForm().isPresent());
        }
    }

    @Test
    void testDeferredTemporalConversionUsesRetainableGuarantee() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy");
        runner.enableControllerService(service);

        try (RecordReader reader = service.createRecordReaderFromBytes(
                Map.of(), "{\"date\":\"2026\"}".getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            final Record record = reader.nextRecord();
            assertThrows(IllegalStateException.class, () -> record.getValue("date"));
        }
    }

    @Test
    void testNonCapturingEagerTemporalFailurePreservesConversionCause() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy");
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.RECORD_MATERIALIZATION_STRATEGY,
                RecordMaterializationStrategy.EAGER.getValue());
        runner.enableControllerService(service);
        final byte[] input = "{\"date\":\"2026\"}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            final MalformedRecordException failure = assertThrows(MalformedRecordException.class, reader::nextRecord);
            assertFalse(failure.getMessage().contains("byte offsets"));
            final org.apache.nifi.serialization.record.field.FieldConversionException conversionFailure = assertInstanceOf(
                    org.apache.nifi.serialization.record.field.FieldConversionException.class, failure.getCause());
            assertTrue(conversionFailure.getMessage().contains("DateTimeParseException"));
        }
    }

    @Test
    void testDirectEagerReaderReportsRetainableBeforeLaterTemporalFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, DateTimeUtils.DATE_FORMAT, "yyyy");
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.RECORD_MATERIALIZATION_STRATEGY,
                RecordMaterializationStrategy.EAGER.getValue());
        runner.enableControllerService(service);
        final byte[] input = "[{\"id\":1},{\"id\":2,\"date\":\"2026\"}]".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertEquals(1, reader.nextRecord().getValue("id"));
            assertThrows(MalformedRecordException.class, reader::nextRecord);
        }
    }

    @Test
    void testStaticSchemaCoercionAliasAndDefaultUseTypedWriterOutput() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(service, SCHEMA_TEXT, """
                {"type":"record","name":"event","fields":[
                  {"name":"id","type":"int","aliases":["identifier"]},
                  {"name":"status","type":"string","default":"active"}
                ]}
                """);
        runner.enableControllerService(service);
        final byte[] input = "{\"identifier\":\"42\"}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (StreamingJsonWriteResult writer = new StreamingJsonWriteResult(runner.getLogger(), record.getSchema(),
                    new SchemaNameAsAttribute(), output, false, NullSuppression.NEVER_SUPPRESS,
                    OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                    RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
                writer.write(RecordSet.of(record.getSchema(), record));
            }

            assertEquals("[{\"id\":42,\"status\":\"active\"}]", output.toString(StandardCharsets.UTF_8));
            assertTrue(record.isMaterialized());
        }
    }

    @Test
    void testInactiveEagerMaterializationPropertyDoesNotDisableStaticSchemaCapture() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.RECORD_MATERIALIZATION_STRATEGY,
                RecordMaterializationStrategy.EAGER.getValue());
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(service, SCHEMA_TEXT,
                "{\"type\":\"record\",\"name\":\"event\",\"fields\":[{\"name\":\"number\",\"type\":\"int\"}]}");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            final Record record = reader.nextRecord();
            assertEquals(42, record.getValue("number"));
            assertTrue(record.getSerializedForm().isPresent());
        }
    }

    @Test
    void testValidatedReaderSupportsMultipleRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "[ {\"number\":42},\n{\"number\":43} ]".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            final Record first = reader.nextRecord();
            assertEquals(42, first.getValue("number"));
            assertTrue(((Utf8JsonValue) first.getSerializedForm().orElseThrow().getSerialized()).isBackedBy(input));
            final Record second = reader.nextRecord();
            assertEquals(43, second.getValue("number"));
            assertTrue(((Utf8JsonValue) second.getSerializedForm().orElseThrow().getSerialized()).isBackedBy(input));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testValidatedRecordsMaterializeOutOfOrderAfterReaderClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "[{\"number\":42},{\"number\":43}]".getBytes(StandardCharsets.UTF_8);
        final Record first;
        final Record second;

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            first = reader.nextRecord();
            second = reader.nextRecord();
        }

        assertEquals(43, second.getValue("number"));
        assertEquals(42, first.getValue("number"));
    }

    @Test
    void testInputStreamValidatedRecordsMaterializeOutOfOrderAfterReaderClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = " [ {\"text\":\"café\"},\r\n{\"text\":\"東京\"} ] ".getBytes(StandardCharsets.UTF_8);
        final DeferredJsonRecord first;
        final DeferredJsonRecord second;

        try (RecordReader reader = service.createRecordReader(Map.of(), new OneByteMarkInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            first = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            second = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            assertFalse(first.isMaterialized());
            assertFalse(second.isMaterialized());
            assertFalse(assertInstanceOf(Utf8JsonValue.class,
                    first.getSerializedForm().orElseThrow().getSerialized()).isBackedBy(input));
            assertEquals("{\"text\":\"café\"}", first.getSerializedForm().orElseThrow().getSerialized().toString());
            assertEquals("{\"text\":\"東京\"}", second.getSerializedForm().orElseThrow().getSerialized().toString());
            assertNull(reader.nextRecord());
        }

        assertEquals("東京", second.getAsString("text"));
        assertEquals("café", first.getAsString("text"));
    }

    @Test
    void testInputStreamValidatedRecordNormalizesDuplicateFieldsLazily() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":1,\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            assertEquals("{\"number\":42}", record.getSerializedForm().orElseThrow().getSerialized().toString());
            assertFalse(record.isMaterialized());
            assertEquals(42, record.getValue("number"));
        }
    }

    @Test
    void testInputStreamValidatedReaderReportsSecondPassFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("number", RecordFieldType.INT.getDataType())));

        try (RecordReader reader = new ValidatedInputStreamRecordReader(new FailingInputStream(input, 5), runner.getLogger(), schema,
                List.of(metadata(0, input.length)), null, null, null, new StreamingJsonParserFactory())) {
            final IOException failure = assertThrows(IOException.class, reader::nextRecord);
            assertEquals("second pass failed", failure.getMessage());
            final IOException terminalFailure = assertThrows(IOException.class, reader::nextRecord);
            assertEquals("Record Reader cannot continue after a validated JSON second-pass failure", terminalFailure.getMessage());
        }
    }

    @Test
    void testInputStreamDeferredMetadataByteLimits() {
        final long recordLimit = ValidatedInputStreamRecordReader.MAX_DEFERRED_RECORD_BYTES;
        final StreamingJsonSchemaInference.JsonRecordMetadata oversized = metadata(0, recordLimit + 1);
        assertFalse(ValidatedInputStreamRecordReader.isMetadataSupported(List.of(oversized)));

        final List<StreamingJsonSchemaInference.JsonRecordMetadata> bounded = List.of(
                metadata(0, recordLimit), metadata(recordLimit, recordLimit * 2),
                metadata(recordLimit * 2, recordLimit * 3), metadata(recordLimit * 3, recordLimit * 4));
        assertTrue(ValidatedInputStreamRecordReader.isMetadataSupported(bounded));

        final List<StreamingJsonSchemaInference.JsonRecordMetadata> aggregateOversized = List.of(
                metadata(0, recordLimit), metadata(recordLimit, recordLimit * 2),
                metadata(recordLimit * 2, recordLimit * 3), metadata(recordLimit * 3, recordLimit * 4),
                metadata(recordLimit * 4, recordLimit * 5));
        assertFalse(ValidatedInputStreamRecordReader.isMetadataSupported(aggregateOversized));
    }

    @Test
    void testInputStreamRecordBeyondDeferredByteLimitUsesEagerReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] prefix = "{\"value\":\"".getBytes(StandardCharsets.UTF_8);
        final byte[] input = new byte[ValidatedInputStreamRecordReader.MAX_DEFERRED_RECORD_BYTES + 1];
        System.arraycopy(prefix, 0, input, 0, prefix.length);
        java.util.Arrays.fill(input, prefix.length, input.length - 2, (byte) 'x');
        input[input.length - 2] = '"';
        input[input.length - 1] = '}';

        try (RecordReader reader = service.createRecordReader(
                Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            final Record record = reader.nextRecord();
            assertEquals(input.length - prefix.length - 2, record.getAsString("value").length());
            assertFalse(record.getSerializedForm().isPresent());
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testCachedSchemaCaptureLimitContinuesWithFollowingRecord() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final TestSchemaCache schemaCache = new TestSchemaCache(schema);
        runner.addControllerService("schema-cache", schemaCache);
        runner.enableControllerService(schemaCache);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, SCHEMA_CACHE, "schema-cache");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        final byte[] prefix = "[{\"value\":\"".getBytes(StandardCharsets.UTF_8);
        final byte[] suffix = "\"},{\"value\":\"small\"}]".getBytes(StandardCharsets.UTF_8);
        final byte[] input = new byte[prefix.length + RecordCapturingInputStream.MAX_RECORD_CAPTURE_BYTES + suffix.length];
        System.arraycopy(prefix, 0, input, 0, prefix.length);
        java.util.Arrays.fill(input, prefix.length, input.length - suffix.length, (byte) 'x');
        System.arraycopy(suffix, 0, input, input.length - suffix.length, suffix.length);

        final Map<String, String> variables = Map.of(RecordSchemaCacheService.CACHE_IDENTIFIER_ATTRIBUTE, "test");
        try (RecordReader reader = service.createRecordReader(variables, new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            final Record oversized = reader.nextRecord();
            assertEquals(RecordCapturingInputStream.MAX_RECORD_CAPTURE_BYTES, oversized.getAsString("value").length());
            assertFalse(oversized.getSerializedForm().isPresent());

            final Record following = reader.nextRecord();
            assertEquals("small", following.getAsString("value"));
            assertTrue(following.getSerializedForm().isPresent());
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testMalformedTrailingInputFailsBeforeReaderIsReturned() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":42}{\"number\":".getBytes(StandardCharsets.UTF_8);

        assertThrows(IOException.class, () -> service.createRecordReader(
                Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger()));
    }

    @Test
    void testValidatedReaderReusesSerializedFormContainingNull() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"value\":null}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            assertTrue(record.getSerializedForm().isPresent());
            assertFalse(record.isMaterialized());
        }
    }

    @Test
    void testValidatedReaderNormalizesDuplicateFields() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":1,\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            final DeferredJsonRecord record = assertInstanceOf(DeferredJsonRecord.class, reader.nextRecord());
            assertEquals("{\"number\":42}", record.getSerializedForm().orElseThrow().getSerialized().toString());
            assertEquals(42, record.getValue("number"));
            assertEquals("{\"number\":42}", record.getSerializedForm().orElseThrow().getSerialized().toString());
        }
    }

    @Test
    void testLargeRecordSetUsesBoundedEagerReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final String input = "[" + "{\"number\":42},".repeat(TestStreamingJsonRecordReaderService.MAX_DEFERRED_RECORDS) + "{\"number\":42}]";

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input.getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            int recordCount = 0;
            while (reader.nextRecord() != null) {
                recordCount++;
            }
            assertEquals(TestStreamingJsonRecordReaderService.MAX_DEFERRED_RECORDS + 1, recordCount);
        }
    }

    @Test
    void testCreateRecordReaderFromBytesUsesEagerStreamingForLenientParsing() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.LENIENT);
        final byte[] input = "{'number':42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            final Record record = reader.nextRecord();
            assertNotNull(record);
            assertEquals(42, record.getValue("number"));
        }
    }

    @Test
    void testCreateRecordReaderFromBytesUsesEagerReaderWithSchemaCache() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestSchemaCache schemaCache = new TestSchemaCache();
        runner.addControllerService("schema-cache", schemaCache);
        runner.enableControllerService(schemaCache);

        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, SCHEMA_CACHE, "schema-cache");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testNullVariablesSupportedByByteAndInputStreamFactories() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReaderFromBytes(null, input, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
        try (RecordReader reader = service.createRecordReader(null, new java.io.ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testInputStreamFactoryUsesValidatedDeferredReaderForMultipleRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "[{\"number\":42},{\"number\":43}]".getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReader(Map.of(), new java.io.ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            final Record first = reader.nextRecord();
            final Record second = reader.nextRecord();
            assertEquals(42, first.getValue("number"));
            assertEquals(43, second.getValue("number"));
            final Utf8JsonValue firstJson = assertInstanceOf(Utf8JsonValue.class,
                    first.getSerializedForm().orElseThrow().getSerialized());
            final Utf8JsonValue secondJson = assertInstanceOf(Utf8JsonValue.class,
                    second.getSerializedForm().orElseThrow().getSerialized());
            assertEquals("{\"number\":42}", firstJson.toString());
            assertEquals("{\"number\":43}", secondJson.toString());
            assertFalse(firstJson.isBackedBy(input));
            assertFalse(secondJson.isBackedBy(input));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testInputStreamRecordCountBeyondMetadataLimitUsesValidatedEagerReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final String json = "[" + "{\"number\":42},".repeat(TestStreamingJsonRecordReaderService.MAX_DEFERRED_RECORDS)
                + "{\"number\":42}]";
        final byte[] input = json.getBytes(StandardCharsets.UTF_8);

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            int count = 0;
            while (reader.nextRecord() != null) {
                count++;
            }
            assertEquals(TestStreamingJsonRecordReaderService.MAX_DEFERRED_RECORDS + 1, count);
        }
    }

    @Test
    void testUtf8BomInputStreamUsesNormalizedEagerReader() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] json = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final byte[] input = new byte[json.length + 3];
        input[0] = (byte) 0xEF;
        input[1] = (byte) 0xBB;
        input[2] = (byte) 0xBF;
        System.arraycopy(json, 0, input, 3, json.length);

        try (RecordReader reader = service.createRecordReader(Map.of(), new ByteArrayInputStream(input), input.length, runner.getLogger())) {
            assertInstanceOf(StreamingJsonRowRecordReader.class, reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testNonRewindableInputStreamInferenceUsesBoundedReplay() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final NonRewindableInputStream input = new NonRewindableInputStream("{\"number\":42}".getBytes(StandardCharsets.UTF_8));

        try (RecordReader reader = service.createRecordReader(Map.of(), input, -1, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
            assertNull(reader.nextRecord());
        }

        assertTrue(input.bytesRead > 0);
        assertTrue(input.closed);
    }

    @Test
    void testConfiguredSchemaInferenceReplayLimitIsEnforced() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.MAX_SCHEMA_INFERENCE_REPLAY_SIZE, "32 B");
        runner.enableControllerService(service);
        final NonRewindableInputStream input = new NonRewindableInputStream(
                "{\"value\":\"content larger than the configured replay limit\"}".getBytes(StandardCharsets.UTF_8));

        final IOException failure = assertThrows(IOException.class,
                () -> service.createRecordReader(Map.of(), input, -1, runner.getLogger()));

        assertEquals("Schema inference replay exceeds the bounded capture limit of 32 bytes", failure.getMessage());
        assertTrue(input.closed);
    }

    @Test
    void testLargeNonRewindableInputStreamInferenceSpillsReplayToDisk() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final String value = "x".repeat(ReplayableInputStream.DEFAULT_MEMORY_THRESHOLD_BYTES + 1);
        final byte[] json = ("{\"value\":\"" + value + "\"}").getBytes(StandardCharsets.UTF_8);
        final NonRewindableInputStream input = new NonRewindableInputStream(json);

        try (RecordReader reader = service.createRecordReader(Map.of(), input, -1, runner.getLogger())) {
            assertEquals(value, reader.nextRecord().getAsString("value"));
            assertNull(reader.nextRecord());
        }

        assertTrue(input.closed);
    }

    @Test
    void testLargeFiniteMarkInputStreamUsesBoundedReplay() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final String value = "x".repeat(ReplayableInputStream.DEFAULT_MEMORY_THRESHOLD_BYTES + 1);
        final byte[] json = ("{\"value\":\"" + value + "\"}").getBytes(StandardCharsets.UTF_8);
        final InputStream input = new BufferedInputStream(new ByteArrayInputStream(json), 8192);

        try (RecordReader reader = service.createRecordReader(Map.of(), input, json.length, runner.getLogger())) {
            assertInstanceOf(ValidatedInputStreamRecordReader.class, reader);
            assertEquals(value, reader.nextRecord().getAsString("value"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testValidatedInputStreamDiscardsLargeLeadingWhitespaceWithoutDelegateSkip() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final byte[] record = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final byte[] input = new byte[1024 * 1024 + record.length];
        java.util.Arrays.fill(input, 0, input.length - record.length, (byte) ' ');
        System.arraycopy(record, 0, input, input.length - record.length, record.length);
        final NoSkipInputStream stream = new NoSkipInputStream(input);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("number", RecordFieldType.INT.getDataType())));

        try (RecordReader reader = new ValidatedInputStreamRecordReader(stream, runner.getLogger(), schema,
                List.of(metadata(input.length - record.length, input.length)), null, null, null, new StreamingJsonParserFactory())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
        }

        assertEquals(0, stream.skipCalls);
        assertTrue(stream.bulkReadCalls <= 130, "Large whitespace discard should use the reusable bulk buffer");
    }

    @Test
    void testValidatedInputStreamRetriesFailedClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final byte[] content = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final FailOnceCloseInputStream input = new FailOnceCloseInputStream(content);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("number", RecordFieldType.INT.getDataType())));
        final RecordReader reader = new ValidatedInputStreamRecordReader(input, runner.getLogger(), schema,
                List.of(metadata(0, content.length)), null, null, null, new StreamingJsonParserFactory());
        assertEquals(42, reader.nextRecord().getValue("number"));

        assertThrows(IOException.class, reader::close);
        assertThrows(IOException.class, reader::nextRecord);
        reader.close();

        assertEquals(2, input.closeAttempts);
    }

    @Test
    void testReplayOwningReaderRetriesFailedSourceClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final FailOnceCloseInputStream input = new FailOnceCloseInputStream("{\"number\":42}".getBytes(StandardCharsets.UTF_16BE));
        final RecordReader reader = service.createRecordReader(Map.of(), input, -1, runner.getLogger());
        assertInstanceOf(ReplayOwningRecordReader.class, reader);
        assertEquals(42, reader.nextRecord().getValue("number"));

        assertThrows(IOException.class, reader::close);
        assertThrows(IOException.class, reader::nextRecord);
        reader.close();

        assertEquals(2, input.closeAttempts);
    }

    @Test
    void testStreamingReaderRetriesFailedSourceClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final byte[] content = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final FailOnceCloseInputStream input = new FailOnceCloseInputStream(content);
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("number", RecordFieldType.INT.getDataType())));
        final RecordReader reader = new StreamingJsonRowRecordReader(input, runner.getLogger(), schema, null, null, null,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, new StreamingJsonParserFactory(), false);

        assertThrows(IOException.class, reader::close);
        reader.close();

        assertEquals(2, input.closeAttempts);
    }

    @Test
    void testTreeReaderRetriesFailedSourceClose() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final byte[] content = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        final FailOnceCloseInputStream input = new FailOnceCloseInputStream(content);
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("number", RecordFieldType.INT.getDataType())));
        final RecordReader reader = new JsonTreeRowRecordReader(input, runner.getLogger(), schema, null, null, null,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, null, new StreamingJsonParserFactory());

        assertThrows(IOException.class, reader::close);
        reader.close();

        assertEquals(2, input.closeAttempts);
    }

    @Test
    void testNonRewindableInputStreamUsesCachedSchemaWithoutReplay() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("number", RecordFieldType.INT.getDataType())));
        final TestSchemaCache schemaCache = new TestSchemaCache(schema);
        runner.addControllerService("schema-cache", schemaCache);
        runner.enableControllerService(schemaCache);

        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, SCHEMA_CACHE, "schema-cache");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        final NonRewindableInputStream input = new NonRewindableInputStream("{\"number\":42}".getBytes(StandardCharsets.UTF_8));
        final Map<String, String> variables = Map.of(RecordSchemaCacheService.CACHE_IDENTIFIER_ATTRIBUTE, "test");
        try (RecordReader reader = service.createRecordReader(variables, input, -1, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
            assertNull(reader.nextRecord());
        }
        assertTrue(input.closed);
    }

    @Test
    void testInferenceFailureIsNotMaskedByRuntimeReplayCloseFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final IllegalStateException closeFailure = new IllegalStateException("close failed");
        final InputStream input = new InputStream() {
            private final InputStream delegate = new ByteArrayInputStream("{\"number\":".getBytes(StandardCharsets.UTF_8));

            @Override
            public int read() throws IOException {
                return delegate.read();
            }

            @Override
            public int read(final byte[] buffer, final int offset, final int length) throws IOException {
                return delegate.read(buffer, offset, length);
            }

            @Override
            public void close() {
                throw closeFailure;
            }
        };

        final IOException thrown = assertThrows(IOException.class,
                () -> service.createRecordReader(Map.of(), input, -1, runner.getLogger()));

        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void testFiniteByteArraySubclassUsesBoundedReplay() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = enableReader(runner, ParsingStrategy.STANDARD);
        final ForwardingMarkInputStream input = new ForwardingMarkInputStream("{\"number\":42}".getBytes(StandardCharsets.UTF_8));

        try (RecordReader reader = service.createRecordReader(Map.of(), input, -1, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
            assertNull(reader.nextRecord());
        }

        assertEquals(0, input.resets);
        assertTrue(input.closed);
    }

    @Test
    void testNullVariablesSupportedWithSchemaCache() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestSchemaCache schemaCache = new TestSchemaCache();
        runner.addControllerService("schema-cache", schemaCache);
        runner.enableControllerService(schemaCache);

        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, SCHEMA_CACHE, "schema-cache");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        try (RecordReader reader = service.createRecordReaderFromBytes(null, input, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testNestedFieldNameRequiredWhenNestedFieldStrategySelected() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, TestStreamingJsonRecordReaderService.STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.getValue());

        runner.assertNotValid(service);
        runner.setProperty(service, TestStreamingJsonRecordReaderService.STARTING_FIELD_NAME, "events");
        runner.assertValid(service);
    }

    @Test
    void testMaximumStringLengthRejectsIntegerOverflow() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractJsonRowRecordReader.MAX_STRING_LENGTH, "2147483648 B");

        runner.assertNotValid(service);
        runner.setProperty(service, AbstractJsonRowRecordReader.MAX_STRING_LENGTH, "2147483647 B");
        runner.assertValid(service);
    }

    @Test
    void testSchemaInferenceFieldLimitRejectsHighCardinalityObject() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.MAX_SCHEMA_INFERENCE_FIELDS, "2");
        runner.enableControllerService(service);

        final IOException failure = assertThrows(IOException.class, () -> service.createRecordReaderFromBytes(
                Map.of(), "{\"a\":1,\"b\":2,\"c\":3}".getBytes(StandardCharsets.UTF_8), runner.getLogger()));

        assertTrue(failure.getMessage().contains("field limit of 2"));
    }

    @Test
    void testMaximumNestingDepthRejectsDeepInput() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractStreamingJsonRecordReaderService.MAX_NESTING_DEPTH, "3");
        runner.enableControllerService(service);

        assertThrows(IOException.class, () -> service.createRecordReaderFromBytes(
                Map.of(), "{\"a\":{\"b\":{\"c\":{\"d\":1}}}}".getBytes(StandardCharsets.UTF_8), runner.getLogger()));
    }

    @Test
    void testContentEncodedSchemaReferenceConsumesHeaderFromRecordStream() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("event", new SimpleRecordSchema(List.of(new RecordField("number", RecordFieldType.INT.getDataType()))));
        final PrefixSchemaReferenceReader referenceReader = new PrefixSchemaReferenceReader();
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        runner.addControllerService("registry", registry);
        runner.addControllerService("reference-reader", referenceReader);
        runner.addControllerService("reader", service);
        runner.enableControllerService(registry);
        runner.enableControllerService(referenceReader);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY.getValue());
        runner.setProperty(service, SCHEMA_REGISTRY, "registry");
        runner.setProperty(service, SCHEMA_REFERENCE_READER, "reference-reader");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        final byte[] input = "!{\"number\":42}".getBytes(StandardCharsets.UTF_8);
        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertEquals(42, reader.nextRecord().getValue("number"));
            assertNull(reader.nextRecord());
        }
    }

    @ParameterizedTest
    @MethodSource("migrationConfigurations")
    void testMigrateProperties(MockPropertyConfiguration configuration, Set<String> expectedRemoved) {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("starting-field-strategy", TestStreamingJsonRecordReaderService.STARTING_FIELD_STRATEGY.getName()),
                Map.entry("starting-field-name", TestStreamingJsonRecordReaderService.STARTING_FIELD_NAME.getName()),
                Map.entry("schema-application-strategy", TestStreamingJsonRecordReaderService.SCHEMA_APPLICATION_STRATEGY.getName()),
                Map.entry(OBSOLETE_SCHEMA_CACHE, SCHEMA_CACHE.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME, SCHEMA_ACCESS_STRATEGY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REGISTRY_PROPERTY_NAME, SCHEMA_REGISTRY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_NAME_PROPERTY_NAME, SCHEMA_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME, SCHEMA_BRANCH_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_VERSION_PROPERTY_NAME, SCHEMA_VERSION.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_TEXT_PROPERTY_NAME, SCHEMA_TEXT.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME, SCHEMA_REFERENCE_READER.getName())
        );

        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        service.migrateProperties(configuration);
        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();

        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();
        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<String> propertiesRemoved = result.getPropertiesRemoved();
        assertEquals(expectedRemoved, propertiesRemoved);
    }

    private static Stream<Arguments> migrationConfigurations() {
        return Stream.of(
                Arguments.argumentSet("Configuration without allow comments", new MockPropertyConfiguration(Map.of()), Set.of()),
                Arguments.argumentSet("Configuration with allow comments",
                        new MockPropertyConfiguration(Map.of(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS, "true")), Set.of(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS))
        );
    }

    private TestStreamingJsonRecordReaderService enableReader(final TestRunner runner, final ParsingStrategy parsingStrategy) throws Exception {
        final TestStreamingJsonRecordReaderService service = new TestStreamingJsonRecordReaderService();
        enableReader(runner, service, parsingStrategy);
        return service;
    }

    private void enableReader(final TestRunner runner, final TestStreamingJsonRecordReaderService service, final ParsingStrategy parsingStrategy) throws Exception {
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, parsingStrategy.getValue());
        runner.enableControllerService(service);
    }

    private static final class TestStreamingJsonRecordReaderService extends AbstractStreamingJsonRecordReaderService {
        private RecordReader createRecordReaderFromBytes(final Map<String, String> variables, final byte[] input,
                                                         final org.apache.nifi.logging.ComponentLog logger)
                throws IOException, MalformedRecordException, SchemaNotFoundException {
            return createRecordReaderFromBytesInternal(variables, input, logger);
        }
    }

    private static final class NonRewindableInputStream extends InputStream {
        private final InputStream delegate;
        private int bytesRead;
        private boolean closed;

        private NonRewindableInputStream(final byte[] content) {
            delegate = new java.io.ByteArrayInputStream(content);
        }

        @Override
        public int read() throws IOException {
            final int value = delegate.read();
            if (value >= 0) {
                bytesRead++;
            }
            return value;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) throws IOException {
            final int count = delegate.read(buffer, offset, length);
            if (count > 0) {
                bytesRead += count;
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            delegate.close();
        }
    }

    private static final class ForwardingMarkInputStream extends ByteArrayInputStream {
        private int resets;
        private boolean closed;

        private ForwardingMarkInputStream(final byte[] content) {
            super(content);
        }

        @Override
        public synchronized void reset() {
            resets++;
            super.reset();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static final class OneByteMarkInputStream extends ByteArrayInputStream {
        private OneByteMarkInputStream(final byte[] content) {
            super(content);
        }

        @Override
        public synchronized int read(final byte[] buffer, final int offset, final int length) {
            return super.read(buffer, offset, Math.min(length, 1));
        }
    }

    private static final class FailingInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private final int readLimit;
        private int bytesRead;

        private FailingInputStream(final byte[] content, final int readLimit) {
            delegate = new ByteArrayInputStream(content);
            this.readLimit = readLimit;
        }

        @Override
        public int read() throws IOException {
            if (bytesRead == readLimit) {
                throw new IOException("second pass failed");
            }
            final int value = delegate.read();
            if (value >= 0) {
                bytesRead++;
            }
            return value;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) throws IOException {
            if (bytesRead == readLimit) {
                throw new IOException("second pass failed");
            }
            final int count = delegate.read(buffer, offset, Math.min(length, readLimit - bytesRead));
            if (count > 0) {
                bytesRead += count;
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class NoSkipInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private int bulkReadCalls;
        private int skipCalls;

        private NoSkipInputStream(final byte[] content) {
            delegate = new ByteArrayInputStream(content);
        }

        @Override
        public int read() {
            return delegate.read();
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) {
            bulkReadCalls++;
            return delegate.read(buffer, offset, length);
        }

        @Override
        public long skip(final long length) {
            skipCalls++;
            throw new AssertionError("Validated reader must not delegate skip");
        }
    }

    private static final class FailOnceCloseInputStream extends ByteArrayInputStream {
        private int closeAttempts;

        private FailOnceCloseInputStream(final byte[] content) {
            super(content);
        }

        @Override
        public void close() throws IOException {
            closeAttempts++;
            if (closeAttempts == 1) {
                throw new IOException("close failed");
            }
            super.close();
        }
    }

    private static StreamingJsonSchemaInference.JsonRecordMetadata metadata(final long start, final long end) {
        return new StreamingJsonSchemaInference.JsonRecordMetadata(start, end, false, false, true, false);
    }

    private static final class PrefixSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {
        @Override
        public SchemaIdentifier getSchemaIdentifier(final Map<String, String> variables, final InputStream contentStream) throws IOException {
            if (contentStream.read() != '!') {
                throw new IOException("Schema reference prefix not found");
            }
            return SchemaIdentifier.builder().name("event").build();
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return Set.of(SchemaField.SCHEMA_NAME);
        }
    }

    private static final class TestSchemaCache extends AbstractControllerService implements RecordSchemaCacheService {
        private final Optional<RecordSchema> schema;

        private TestSchemaCache() {
            this.schema = Optional.empty();
        }

        private TestSchemaCache(final RecordSchema schema) {
            this.schema = Optional.of(schema);
        }

        @Override
        public String cacheSchema(final RecordSchema schema) {
            return "test";
        }

        @Override
        public Optional<RecordSchema> getSchema(final String schemaIdentifier) {
            return schema;
        }
    }

}
