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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class TestStreamingJsonRowRecordReader {
    private static final String DATE_FORMAT = RecordFieldType.DATE.getDefaultFormat();
    private static final String TIME_FORMAT = RecordFieldType.TIME.getDefaultFormat();
    private static final String TIMESTAMP_FORMAT = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private final ComponentLog logger = mock(ComponentLog.class);
    private final TokenParserFactory parserFactory = new StreamingJsonParserFactory();

    @Test
    void testRepresentativeRecordParity() throws Exception {
        final RecordSchema locationSchema = new SimpleRecordSchema(List.of(
                new RecordField("latitude", RecordFieldType.DOUBLE.getDataType()),
                new RecordField("longitude", RecordFieldType.DOUBLE.getDataType())));
        final DataType locationType = RecordFieldType.RECORD.getRecordDataType(locationSchema);
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.LONG.getDataType()),
                new RecordField("active", RecordFieldType.BOOLEAN.getDataType()),
                new RecordField("created", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("samples", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())),
                new RecordField("metrics", RecordFieldType.MAP.getMapDataType(RecordFieldType.DOUBLE.getDataType())),
                new RecordField("location", locationType)));
        final String json = """
                {"id":"42","active":true,"created":"2026-08-27 10:15:30",\
                "samples":[1,2,3],"metrics":{"cpu":0.42,"disk":0.81},\
                "location":{"latitude":37.7749,"longitude":-122.4194}}
                """.strip();

        assertParity(json, schema, true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testTypedMapConversionFailureFieldParity() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("metrics", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType()))));
        final byte[] input = "{\"metrics\":{\"cpu\":\"invalid\"}}".getBytes(StandardCharsets.UTF_8);

        final MalformedRecordException treeFailure;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            treeFailure = assertThrows(MalformedRecordException.class, reader::nextRecord);
        }

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            final MalformedRecordException streamingFailure = assertThrows(MalformedRecordException.class, reader::nextRecord);
            assertEquals(treeFailure.getCause().getMessage(), streamingFailure.getCause().getMessage());
        }

        try (final StreamingJsonRowRecordReader reader = createStreaming(new ByteArrayInputStream(input), schema, parserFactory)) {
            final MalformedRecordException streamingFailure = assertThrows(MalformedRecordException.class, reader::nextRecord);
            assertEquals(treeFailure.getCause().getMessage(), streamingFailure.getCause().getMessage());
        }

        try (final StreamingJsonRowRecordReader reader = createNonCapturingStreaming(new ByteArrayInputStream(input), schema)) {
            final MalformedRecordException streamingFailure = assertThrows(MalformedRecordException.class, reader::nextRecord);
            assertEquals(treeFailure.getCause().getMessage(), streamingFailure.getCause().getMessage());
        }
    }

    @Test
    void testInputStreamConversionFailureDoesNotMaskMalformedTail() throws Exception {
        final RecordSchema arraySchema = new SimpleRecordSchema(List.of(
                new RecordField("array", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()))));
        assertMalformedTailParity("{\"array\":[\"bad\"],\"tail\":", arraySchema);
        assertMalformedTailParity("{\"array\":1,\"tail\":", arraySchema);

        final RecordSchema mapSchema = new SimpleRecordSchema(List.of(
                new RecordField("map", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType()))));
        assertMalformedTailParity("{\"map\":{\"key\":\"bad\"},\"tail\":", mapSchema);

        final RecordSchema childSchema = new SimpleRecordSchema(List.of(
                new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField("child", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        assertMalformedTailParity("{\"child\":{\"value\":\"bad\"},\"tail\":", nestedSchema);
    }

    @Test
    void testNonCapturingInputStreamContinuesAfterConversionFailure() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("array", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()))));
        final byte[] input = "[{\"array\":[\"bad\"]},{\"array\":[42]}]".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createNonCapturingStreaming(new ByteArrayInputStream(input), schema)) {
            assertThrows(MalformedRecordException.class, reader::nextRecord);
            assertEquals(42, ((Object[]) reader.nextRecord().getValue("array"))[0]);
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testTreeSchemaSelectionFailsBeforeParserCreation() {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("value", RecordFieldType.STRING.getDataType())));
        final TokenParserFactory unusedParserFactory = mock(TokenParserFactory.class);

        assertThrows(IllegalArgumentException.class, () -> new JsonTreeRowRecordReader(
                new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8)), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.NESTED_FIELD, "missing",
                SchemaApplicationStrategy.WHOLE_JSON, null, unusedParserFactory));

        verifyNoInteractions(unusedParserFactory);
    }

    @Test
    void testRootArrayAndJsonLinesParity() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())));

        assertParity("[{\"id\":1,\"name\":\"one\"},{\"id\":2,\"name\":\"two\"}]", schema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        assertParity("{\"id\":1,\"name\":\"one\"}\r\n{\"id\":2,\"name\":\"two\"}", schema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testRootArraySkipsNonRecordElementsWithoutConsumingFollowingRecords() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "[{\"id\":1},0,null,{\"id\":2}]".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            assertEquals(1, reader.nextRecord().getAsInt("id"));
            assertEquals(2, reader.nextRecord().getAsInt("id"));
            assertNull(reader.nextRecord());
        }

        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            assertEquals(1, reader.nextRecord().getAsInt("id"));
            assertEquals(2, reader.nextRecord().getAsInt("id"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testInputStreamFailureRemainsIOException() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final IOException failure = new IOException("synthetic input failure");
        final byte[] content = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
        final InputStream input = new InputStream() {
            private boolean supplied;

            @Override
            public int read() throws IOException {
                final byte[] singleByte = new byte[1];
                final int count = read(singleByte, 0, 1);
                return count < 0 ? -1 : singleByte[0] & 0xff;
            }

            @Override
            public int read(final byte[] buffer, final int offset, final int length) throws IOException {
                if (supplied) {
                    throw failure;
                }
                supplied = true;
                System.arraycopy(content, 0, buffer, offset, content.length);
                return content.length;
            }
        };

        try (final StreamingJsonRowRecordReader reader = new StreamingJsonRowRecordReader(input, logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, parserFactory)) {
            assertEquals(1, reader.nextRecord().getAsInt("id"));
            assertSame(failure, assertThrows(IOException.class, reader::nextRecord));
        }
    }

    @Test
    void testMidRecordInputStreamFailureRemainsIOException() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final IOException failure = new IOException("mid-record source failure");
        final byte[] content = "{\"id\":123}".getBytes(StandardCharsets.UTF_8);
        final InputStream input = new InputStream() {
            private int position;

            @Override
            public int read() throws IOException {
                if (position == 6) {
                    throw failure;
                }
                return position == content.length ? -1 : content[position++] & 0xff;
            }

            @Override
            public int read(final byte[] buffer, final int offset, final int length) throws IOException {
                if (length == 0) {
                    return 0;
                }
                final int value = read();
                if (value < 0) {
                    return -1;
                }
                buffer[offset] = (byte) value;
                return 1;
            }
        };

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, parserFactory)) {
            assertSame(failure, assertThrows(IOException.class, reader::nextRecord));
        }
    }

    @Test
    void testBatchRecordsShareSourceBytes() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "[{\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            final Utf8JsonValue first = assertInstanceOf(Utf8JsonValue.class,
                    reader.nextRecord().getSerializedForm().orElseThrow().getSerialized());
            final Utf8JsonValue second = assertInstanceOf(Utf8JsonValue.class,
                    reader.nextRecord().getSerializedForm().orElseThrow().getSerialized());

            assertTrue(first.isBackedBy(input));
            assertTrue(second.isBackedBy(input));
        }
    }

    @Test
    void testInputStreamCaptureUsesExactRootArrayAndJsonLinesBoundaries() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType())));

        assertInputStreamSerializedValues(" \n [ {\"id\":1,\"name\":\"one\"} ,\r\n{\"id\":2,\"name\":\"two\"} ] ", schema,
                List.of("{\"id\":1,\"name\":\"one\"}", "{\"id\":2,\"name\":\"two\"}"));
        assertInputStreamSerializedValues("\n{\"id\":1,\"name\":\"one\"}\r\n \t{\"id\":2,\"name\":\"two\"}\n", schema,
                List.of("{\"id\":1,\"name\":\"one\"}", "{\"id\":2,\"name\":\"two\"}"));
    }

    @Test
    void testInputStreamCaptureHandlesOneByteReadsAndMultibyteUtf8() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("name", RecordFieldType.STRING.getDataType())));
        final byte[] input = "[{\"name\":\"Jöhn 🐘\"},{\"name\":\"東京\"}]".getBytes(StandardCharsets.UTF_8);

        try (StreamingJsonRowRecordReader reader = createStreaming(new OneByteInputStream(input), schema, parserFactory)) {
            final Record first = reader.nextRecord();
            final Record second = reader.nextRecord();
            assertEquals("{\"name\":\"Jöhn 🐘\"}", first.getSerializedForm().orElseThrow().getSerialized().toString());
            assertEquals("{\"name\":\"東京\"}", second.getSerializedForm().orElseThrow().getSerialized().toString());
            assertEquals("Jöhn 🐘", first.getAsString("name"));
            assertEquals("東京", second.getAsString("name"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testInputStreamCapturedRecordsOwnDistinctBytesAfterReaderClose() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "[{\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8);
        final Record first;
        final Record second;
        final Utf8JsonValue firstJson;
        final Utf8JsonValue secondJson;

        try (StreamingJsonRowRecordReader reader = createStreaming(new ByteArrayInputStream(input), schema, parserFactory)) {
            first = reader.nextRecord();
            second = reader.nextRecord();
            firstJson = assertInstanceOf(Utf8JsonValue.class, first.getSerializedForm().orElseThrow().getSerialized());
            secondJson = assertInstanceOf(Utf8JsonValue.class, second.getSerializedForm().orElseThrow().getSerialized());
            assertNotSame(firstJson.asUnquotedUTF8(), secondJson.asUnquotedUTF8());
            final byte[] exposed = firstJson.asUnquotedUTF8();
            exposed[0] = '!';
            assertEquals("{\"id\":1}", firstJson.toString());
        }

        assertEquals("{\"id\":1}", first.getSerializedForm().orElseThrow().getSerialized().toString());
        assertEquals("{\"id\":2}", second.getSerializedForm().orElseThrow().getSerialized().toString());
        assertEquals(1, first.getAsInt("id"));
        assertEquals(2, second.getAsInt("id"));
    }

    @Test
    void testInputStreamCaptureRequiresStrictUtf8AndInvalidatesOnMutation() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] utf8 = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
        try (StreamingJsonRowRecordReader reader = createStreaming(new ByteArrayInputStream(utf8), schema, parserFactory)) {
            final Record record = reader.nextRecord();
            assertTrue(record.getSerializedForm().isPresent());
            record.setValue("id", 2);
            assertTrue(record.getSerializedForm().isEmpty());
        }

        final TokenParserFactory lenientParserFactory = new StreamingJsonParserFactory(
                com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.LENIENT);
        try (StreamingJsonRowRecordReader reader = createStreaming(
                new ByteArrayInputStream("{'id':1}".getBytes(StandardCharsets.UTF_8)), schema, lenientParserFactory)) {
            assertTrue(reader.nextRecord().getSerializedForm().isEmpty());
        }

        try (StreamingJsonRowRecordReader reader = createStreaming(
                new ByteArrayInputStream("{\"id\":1}".getBytes(StandardCharsets.UTF_16)), schema, parserFactory)) {
            final Record record = reader.nextRecord();
            assertEquals(1, record.getAsInt("id"));
            assertTrue(record.getSerializedForm().isEmpty());
        }
    }

    @Test
    void testSerializedFormLineBreakState() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));

        assertFalse(readSerializedValue("{\"id\":1}", schema).containsLineBreak());
        assertTrue(readSerializedValue("{\n  \"id\": 1\n}", schema).containsLineBreak());
    }

    @Test
    void testSerializedFormsRemainAvailableAfterReaderClose() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("child", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        final byte[] input = "[{\"id\":1,\"child\":{\"value\":2}},{\"id\":3,\"child\":{\"value\":4}}]".getBytes(StandardCharsets.UTF_8);

        final Record first;
        final Record second;
        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            first = reader.nextRecord();
            second = reader.nextRecord();
        }

        assertEquals("{\"id\":1,\"child\":{\"value\":2}}", first.getSerializedForm().orElseThrow().getSerialized().toString());
        assertEquals("{\"id\":3,\"child\":{\"value\":4}}", second.getSerializedForm().orElseThrow().getSerialized().toString());
        final Record child = assertInstanceOf(Record.class, first.getValue("child"));
        assertEquals("{\"value\":2}", child.getSerializedForm().orElseThrow().getSerialized().toString());
    }

    @Test
    void testUnknownFieldDoesNotDisableNextRecordSerializedForm() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "[{\"id\":1,\"unknown\":2},{\"id\":2}]".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            assertTrue(reader.nextRecord().getSerializedForm().isEmpty());
            assertEquals("{\"id\":2}", reader.nextRecord().getSerializedForm().orElseThrow().getSerialized().toString());
        }
    }

    @Test
    void testDuplicateRecoveryDoesNotConsumeNextRecord() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "[{\"id\":\"ignored\",\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            assertEquals(1, reader.nextRecord().getAsInt("id"));
            assertEquals(2, reader.nextRecord().getAsInt("id"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testAliasesAndUnknownFieldParity() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("identifier", RecordFieldType.INT.getDataType(), Set.of("id")),
                new RecordField("name", RecordFieldType.STRING.getDataType())));
        final String json = "{\"id\":7,\"name\":\"seven\",\"extra\":{\"nested\":true}}";

        assertParity(json, schema, true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        assertParity(json, schema, false, false, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        assertParity("{\"identifier\":null,\"id\":7,\"name\":\"canonical null wins\"}", schema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        assertParity("{\"identifier\":7,\"id\":8,\"name\":\"canonical wins\"}", schema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testDuplicateAndAliasConversionParity() throws Exception {
        final RecordSchema duplicateSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        assertParity("{\"id\":\"ignored\",\"id\":1}", duplicateSchema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        final byte[] duplicateInput = "{\"id\":\"ignored\",\"id\":1}".getBytes(StandardCharsets.UTF_8);
        try (final StreamingJsonRowRecordReader reader = createStreaming(duplicateInput, duplicateSchema,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART)) {
            assertEquals("{\"id\":1}", reader.nextRecord(true, true).getSerializedForm().orElseThrow().getSerialized().toString());
        }

        final RecordSchema aliasSchema = new SimpleRecordSchema(List.of(
                new RecordField("identifier", RecordFieldType.INT.getDataType(), Set.of("id"))));
        assertParity("{\"identifier\":1,\"id\":\"ignored\"}", aliasSchema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
        assertParity("{\"id\":\"ignored\",\"identifier\":1}", aliasSchema,
                true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testUnknownFieldSerializedFormDoesNotRetainDroppedFields() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("retained", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("child", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        final byte[] input = "{\"id\":1,\"ignored\":2,\"child\":{\"retained\":3,\"removed\":4}}".getBytes(StandardCharsets.UTF_8);

        final String treeSerialized;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            treeSerialized = reader.nextRecord(true, true).getSerializedForm().orElseThrow().getSerialized().toString();
        }

        final Record streamingRecord;
        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            streamingRecord = reader.nextRecord(true, true);
        }

        assertEquals("{\"id\":1,\"child\":{\"retained\":3}}", treeSerialized);
        assertTrue(streamingRecord.getSerializedForm().isEmpty());
        assertEquals(1, streamingRecord.getAsInt("id"));
        final Record child = assertInstanceOf(Record.class, streamingRecord.getValue("child"));
        assertEquals(3, child.getAsInt("retained"));
        assertTrue(child.getSerializedForm().isEmpty());
        assertFalse(child.getSchema().getField("removed").isPresent());
    }

    @Test
    void testRawStructuredValueParity() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("array", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())),
                new RecordField("map", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType())),
                new RecordField("record", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        final String json = "{\"array\":{\"raw\":true},\"map\":7,\"record\":[1,{\"nested\":true}],\"tail\":\"retained\"}";

        assertParity(json, schema, false, false, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testRawNestedRecordSerializedFormParity() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("child", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        final byte[] input = "{\"child\": { \"value\": 1 }}".getBytes(StandardCharsets.UTF_8);

        final String treeSerialized;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            final Record child = assertInstanceOf(Record.class, reader.nextRecord(false, false).getValue("child"));
            treeSerialized = child.getSerializedForm().orElseThrow().getSerialized().toString();
        }

        final String streamingSerialized;
        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            final Record child = assertInstanceOf(Record.class, reader.nextRecord(false, false).getValue("child"));
            streamingSerialized = child.getSerializedForm().orElseThrow().getSerialized().toString();
        }

        assertEquals(treeSerialized, streamingSerialized);
        assertEquals("{\"value\":1}", streamingSerialized);
    }

    @Test
    void testCoercedNestedRecordSerializedFormParity() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("child", RecordFieldType.RECORD.getRecordDataType(childSchema))));
        final byte[] input = "{\"child\": { \"value\": 1 }}".getBytes(StandardCharsets.UTF_8);

        final String treeSerialized;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            final Record child = assertInstanceOf(Record.class, reader.nextRecord(true, true).getValue("child"));
            treeSerialized = child.getSerializedForm().orElseThrow().getSerialized().toString();
        }

        final String streamingSerialized;
        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            final Record child = assertInstanceOf(Record.class, reader.nextRecord(true, true).getValue("child"));
            streamingSerialized = child.getSerializedForm().orElseThrow().getSerialized().toString();
        }

        assertEquals(treeSerialized, streamingSerialized);
        assertEquals("{\"value\":1}", streamingSerialized);
    }

    @Test
    void testCoercedStructuredMismatchParity() throws Exception {
        final RecordSchema childSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.INT.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("map", RecordFieldType.MAP.getMapDataType(RecordFieldType.INT.getDataType())),
                new RecordField("record", RecordFieldType.RECORD.getRecordDataType(childSchema)),
                new RecordField("tail", RecordFieldType.STRING.getDataType())));
        final String json = "{\"map\":7,\"record\":[{\"value\":1}],\"tail\":\"retained\"}";

        assertParity(json, schema, true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testStructuredJsonCoercedToScalarParity() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("object", RecordFieldType.STRING.getDataType()),
                new RecordField("array", RecordFieldType.STRING.getDataType())));
        final String json = "{\"object\":{\"value\":1},\"array\":[1,2]}";

        assertParity(json, schema, true, true, StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testNestedFieldParity() throws Exception {
        final RecordSchema eventSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema rootSchema = new SimpleRecordSchema(List.of(
                new RecordField("events", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(eventSchema)))));
        final String json = "{\"metadata\":\"ignored\",\"events\":[{\"id\":1},{\"id\":2}]}";

        assertParity(json, rootSchema, true, true, StartingFieldStrategy.NESTED_FIELD, "events", SchemaApplicationStrategy.WHOLE_JSON);
    }

    @Test
    void testNestedFieldSelectionSkipsScalarArray() throws Exception {
        final RecordSchema eventSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema wrapperSchema = new SimpleRecordSchema(List.of(
                new RecordField("events", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(eventSchema)))));
        final RecordSchema rootSchema = new SimpleRecordSchema(List.of(
                new RecordField("values", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())),
                new RecordField("wrapper", RecordFieldType.RECORD.getRecordDataType(wrapperSchema))));
        final String json = "{\"values\":[1,2],\"wrapper\":{\"events\":[{\"id\":1}]}}";

        assertParity(json, rootSchema, true, true, StartingFieldStrategy.NESTED_FIELD, "events", SchemaApplicationStrategy.WHOLE_JSON);
    }

    @Test
    void testMalformedTrailingRecordFailsWhenRead() throws Exception {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] input = "{\"id\":1}\n{\"id\":".getBytes(StandardCharsets.UTF_8);

        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART)) {
            assertEquals(1, reader.nextRecord().getAsInt("id"));
            assertThrows(MalformedRecordException.class, reader::nextRecord);
        }
    }

    @Test
    void testChoiceSchemaUsesTreeFallback() {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.INT.getDataType(), RecordFieldType.STRING.getDataType()))));

        assertFalse(StreamingJsonRowRecordReader.isSchemaSupported(schema));
        assertTrue(StreamingJsonRowRecordReader.isSchemaSupported(new SimpleRecordSchema(
                List.of(new RecordField("value", RecordFieldType.STRING.getDataType())))));
    }

    @Test
    void testDeepContainerSchemasUseTreeFallbackAtBound() {
        DataType arrayType = RecordFieldType.STRING.getDataType();
        DataType mapType = RecordFieldType.STRING.getDataType();
        for (int depth = 0; depth < 100; depth++) {
            arrayType = RecordFieldType.ARRAY.getArrayDataType(arrayType);
            mapType = RecordFieldType.MAP.getMapDataType(mapType);
        }

        assertTrue(StreamingJsonRowRecordReader.isSchemaSupported(schemaWithType(arrayType)));
        assertTrue(StreamingJsonRowRecordReader.isSchemaSupported(schemaWithType(mapType)));
        assertFalse(StreamingJsonRowRecordReader.isSchemaSupported(
                schemaWithType(RecordFieldType.ARRAY.getArrayDataType(arrayType))));
        assertFalse(StreamingJsonRowRecordReader.isSchemaSupported(
                schemaWithType(RecordFieldType.MAP.getMapDataType(mapType))));
    }

    @Test
    void testInvalidNestedFieldSelectionRejected() {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final byte[] input = "{\"value\":\"scalar\"}".getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> createStreaming(input, schema, StartingFieldStrategy.NESTED_FIELD, "value",
                SchemaApplicationStrategy.WHOLE_JSON));
    }

    private void assertParity(final String json, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields,
                              final StartingFieldStrategy startingFieldStrategy, final String startingFieldName,
                              final SchemaApplicationStrategy schemaApplicationStrategy) throws Exception {
        final byte[] input = json.getBytes(StandardCharsets.UTF_8);
        final List<Object> treeRecords;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                startingFieldStrategy, startingFieldName, schemaApplicationStrategy, null, parserFactory)) {
            treeRecords = readRecords(reader, coerceTypes, dropUnknownFields);
        }

        final List<Object> streamingRecords;
        try (final StreamingJsonRowRecordReader reader = createStreaming(input, schema, startingFieldStrategy, startingFieldName, schemaApplicationStrategy)) {
            streamingRecords = readRecords(reader, coerceTypes, dropUnknownFields);
        }

        assertEquals(treeRecords, streamingRecords);

        final List<Object> inputStreamRecords;
        try (final StreamingJsonRowRecordReader reader = new StreamingJsonRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, startingFieldStrategy, startingFieldName, schemaApplicationStrategy, parserFactory)) {
            inputStreamRecords = readRecords(reader, coerceTypes, dropUnknownFields);
        }

        assertEquals(treeRecords, inputStreamRecords);

        final List<Object> nonCapturingInputStreamRecords;
        try (final StreamingJsonRowRecordReader reader = new StreamingJsonRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, startingFieldStrategy, startingFieldName, schemaApplicationStrategy, parserFactory, false)) {
            nonCapturingInputStreamRecords = readRecords(reader, coerceTypes, dropUnknownFields);
        }

        assertEquals(treeRecords, nonCapturingInputStreamRecords);
    }

    private StreamingJsonRowRecordReader createStreaming(final byte[] input, final RecordSchema schema,
                                                         final StartingFieldStrategy startingFieldStrategy, final String startingFieldName,
                                                         final SchemaApplicationStrategy schemaApplicationStrategy) throws Exception {
        return new StreamingJsonRowRecordReader(input, logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                startingFieldStrategy, startingFieldName, schemaApplicationStrategy, parserFactory);
    }

    private StreamingJsonRowRecordReader createStreaming(final InputStream input, final RecordSchema schema,
                                                          final TokenParserFactory tokenParserFactory) throws Exception {
        return new StreamingJsonRowRecordReader(input, logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, tokenParserFactory);
    }

    private StreamingJsonRowRecordReader createNonCapturingStreaming(final InputStream input, final RecordSchema schema) throws Exception {
        return new StreamingJsonRowRecordReader(input, logger, schema, DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART, parserFactory, false);
    }

    private void assertInputStreamSerializedValues(final String json, final RecordSchema schema, final List<String> expected) throws Exception {
        final List<String> actual = new ArrayList<>();
        try (StreamingJsonRowRecordReader reader = createStreaming(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), schema, parserFactory)) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                actual.add(record.getSerializedForm().orElseThrow().getSerialized().toString());
            }
        }
        assertEquals(expected, actual);
    }

    private void assertMalformedTailParity(final String json, final RecordSchema schema) throws Exception {
        final byte[] input = json.getBytes(StandardCharsets.UTF_8);
        final MalformedRecordException treeFailure;
        try (final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, null, parserFactory)) {
            treeFailure = assertThrows(MalformedRecordException.class, () -> reader.nextRecord(true, true));
        }

        for (final boolean captureSerializedForm : List.of(true, false)) {
            try (final StreamingJsonRowRecordReader reader = new StreamingJsonRowRecordReader(new ByteArrayInputStream(input), logger, schema,
                    DATE_FORMAT, TIME_FORMAT, TIMESTAMP_FORMAT, StartingFieldStrategy.ROOT_NODE, null,
                    SchemaApplicationStrategy.SELECTED_PART, parserFactory, captureSerializedForm)) {
                final MalformedRecordException streamingFailure = assertThrows(MalformedRecordException.class, () -> reader.nextRecord(true, true));
                assertEquals(treeFailure.getMessage(), streamingFailure.getMessage());
                assertEquals(treeFailure.getCause().getClass(), streamingFailure.getCause().getClass());
            }
        }
    }

    private RecordSchema schemaWithType(final DataType dataType) {
        return new SimpleRecordSchema(List.of(new RecordField("value", dataType)));
    }

    private Utf8JsonValue readSerializedValue(final String json, final RecordSchema schema) throws Exception {
        try (final StreamingJsonRowRecordReader reader = createStreaming(json.getBytes(StandardCharsets.UTF_8), schema,
                StartingFieldStrategy.ROOT_NODE, null, SchemaApplicationStrategy.SELECTED_PART)) {
            return assertInstanceOf(Utf8JsonValue.class, reader.nextRecord().getSerializedForm().orElseThrow().getSerialized());
        }
    }

    private List<Object> readRecords(final RecordReader reader, final boolean coerceTypes, final boolean dropUnknownFields) throws Exception {
        final List<Object> records = new ArrayList<>();
        Record record;
        while ((record = reader.nextRecord(coerceTypes, dropUnknownFields)) != null) {
            records.add(normalize(record));
        }
        return records;
    }

    private Object normalize(final Object value) {
        if (value instanceof final Record record) {
            return normalize(record.toMap());
        }
        if (value instanceof final Map<?, ?> map) {
            final Map<Object, Object> normalized = new LinkedHashMap<>();
            map.forEach((key, mapValue) -> normalized.put(key, normalize(mapValue)));
            return normalized;
        }
        if (value instanceof final Object[] array) {
            final List<Object> normalized = new ArrayList<>(array.length);
            for (final Object element : array) {
                normalized.add(normalize(element));
            }
            return normalized;
        }
        return value;
    }

    private static final class OneByteInputStream extends InputStream {
        private final ByteArrayInputStream delegate;

        private OneByteInputStream(final byte[] input) {
            delegate = new ByteArrayInputStream(input);
        }

        @Override
        public int read() {
            return delegate.read();
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) {
            return delegate.read(buffer, offset, Math.min(1, length));
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
