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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SerializedForm;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStreamingJsonWriteResult {

    @Test
    public void testScientificNotationUsage() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("float", RecordFieldType.FLOAT.getDataType()));
        fields.add(new RecordField("double", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("decimal", RecordFieldType.DECIMAL.getDecimalDataType(5, 10)));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String expectedWithScientificNotation = """
            {"float":-4.2910323,"double":4.51E-7,"decimal":8.0E-8}
            """.trim();
        final String expectedWithScientificNotationArray = "[" + expectedWithScientificNotation + "]";
        final String expectedWithoutScientificNotation = """
            {"float":-4.2910323,"double":0.000000451,"decimal":0.000000080}
            """.trim();
        final String expectedWithoutScientificNotationArray = "[" + expectedWithoutScientificNotation + "]";

        final Map<String, Object> values = Map.of(
            "float", -4.291032244F,
            "double", 0.000000451D,
            "decimal", new BigDecimal("0.000000080")
        );
        final Record record = new MapRecord(schema, values);

        final String withScientificNotation = writeRecord(record, true, false);
        assertEquals(expectedWithScientificNotationArray, withScientificNotation);

        // We cannot be sure of the ordering when writing the raw record
        final String rawWithScientificNotation = writeRecord(record, true, true);
        assertTrue(rawWithScientificNotation.contains("\"float\":-4.2910323"));
        assertTrue(rawWithScientificNotation.contains("\"double\":4.51E-7"));
        assertTrue(rawWithScientificNotation.contains("\"decimal\":8.0E-8"));

        final String withoutScientificNotation = writeRecord(record, false, false);
        assertEquals(expectedWithoutScientificNotationArray, withoutScientificNotation);

        // We cannot be sure of the ordering when writing the raw record
        final String rawWithoutScientificNotation = writeRecord(record, false, true);
        assertTrue(rawWithoutScientificNotation.contains("\"float\":-4.2910323"));
        assertTrue(rawWithoutScientificNotation.contains("\"double\":0.000000451"));
        assertTrue(rawWithoutScientificNotation.contains("\"decimal\":0.000000080"));

        final Record recordWithSerializedForm = new MapRecord(schema, values, SerializedForm.of(expectedWithScientificNotation, "application/json"));
        final String writtenWith = writeRecord(recordWithSerializedForm, true, false);
        assertEquals(expectedWithScientificNotationArray, writtenWith);

        final String writtenWithout = writeRecord(recordWithSerializedForm, false, false);
        assertEquals(expectedWithoutScientificNotationArray, writtenWithout);

        // We cannot be sure of the ordering when writing the raw record
        final String writtenWithoutRaw = writeRecord(recordWithSerializedForm, false, true);
        assertTrue(writtenWithoutRaw.contains("\"float\":-4.2910323"));
        assertTrue(writtenWithoutRaw.contains("\"double\":0.000000451"));
        assertTrue(writtenWithoutRaw.contains("\"decimal\":0.000000080"));

        final Record recordWithSerializedBytes = new MapRecord(schema, values,
                SerializedForm.of(expectedWithScientificNotation.getBytes(StandardCharsets.UTF_8), "application/json"));
        assertEquals(expectedWithScientificNotationArray, writeRecord(recordWithSerializedBytes, true, false));
        assertEquals(expectedWithoutScientificNotationArray, writeRecord(recordWithSerializedBytes, false, false));
    }

    private String writeRecord(final Record record, final boolean allowScientificNotation, final boolean writeRawRecord) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), record.getSchema(), new SchemaNameAsAttribute(), baos, false,
                 NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                 RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(), "application/json", allowScientificNotation)) {

            writer.beginRecordSet();
            if (writeRawRecord) {
                writer.writeRawRecord(record);
            } else {
                writer.write(record);
            }

            writer.finishRecordSet();
            writer.flush();

            return baos.toString(StandardCharsets.UTF_8);
        }
    }

    @Test
    void testDataTypes() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        for (final RecordFieldType fieldType : RecordFieldType.values()) {
            if (fieldType == RecordFieldType.CHOICE) {
                final List<DataType> possibleTypes = new ArrayList<>();
                possibleTypes.add(RecordFieldType.INT.getDataType());
                possibleTypes.add(RecordFieldType.LONG.getDataType());

                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getChoiceDataType(possibleTypes)));
            } else if (fieldType == RecordFieldType.MAP) {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getMapDataType(RecordFieldType.INT.getDataType())));
            } else {
                fields.add(new RecordField(fieldType.name().toLowerCase(), fieldType.getDataType()));
            }
        }
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("height", 48);
        map.put("width", 96);

        final Map<String, Object> valueMap = new LinkedHashMap<>();
        valueMap.put("string", "string");
        valueMap.put("boolean", true);
        valueMap.put("byte", (byte) 1);
        valueMap.put("char", 'c');
        valueMap.put("short", (short) 8);
        valueMap.put("int", 9);
        valueMap.put("bigint", BigInteger.valueOf(8L));
        valueMap.put("long", 8L);
        valueMap.put("float", 8.0F);
        valueMap.put("double", 8.0D);
        valueMap.put("decimal", BigDecimal.valueOf(8.1D));
        valueMap.put("date", Date.valueOf("2017-01-01"));
        valueMap.put("time", Time.valueOf("17:00:00"));
        valueMap.put("timestamp", Timestamp.valueOf("2017-01-01 17:00:00"));
        valueMap.put("record", null);
        valueMap.put("array", null);
        valueMap.put("enum", null);
        valueMap.put("choice", 48L);
        valueMap.put("map", map);
        valueMap.put("uuid", "8bb20bf2-ec41-4b94-80a4-922f4dba009c");

        final Record record = new MapRecord(schema, valueMap);
        final RecordSet rs = RecordSet.of(schema, record);

        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, true,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {

            writer.write(rs);
        }

        final String output = baos.toString(StandardCharsets.UTF_8);

        final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/json/output/dataTypes.json")));
        assertEquals(StringUtils.deleteWhitespace(expected), StringUtils.deleteWhitespace(output));
    }

    @Test
    void testGenericSerializedFormUsesTypedValues() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values1 = new HashMap<>();
        values1.put("name", "John Doe");
        values1.put("age", 42);
        final String serialized1 = """
            {
              "name": "John Doe",
              "age": 42
            }""";
        final SerializedForm serializedForm1 = SerializedForm.of(serialized1, "application/json");
        final Record record1 = new MapRecord(schema, values1, serializedForm1);

        final Map<String, Object> values2 = new HashMap<>();
        values2.put("name", "Jane Doe");
        values2.put("age", 43);

        final String serialized2 = """
            {
              "name": "Jane Doe",
              "age": 43
            }""";
        final SerializedForm serializedForm2 = SerializedForm.of(serialized2, "application/json");
        final Record record2 = new MapRecord(schema, values1, serializedForm2);

        final RecordSet rs = RecordSet.of(schema, record1, record2);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, true,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {

            writer.write(rs);
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertFalse(output.contains("Jane Doe"));
        assertEquals(2, output.split("John Doe", -1).length - 1);
    }

    @Test
    void testGenericRecordSkipsSerializedFormInspection() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final Record record = new MapRecord(schema, Map.of("id", 42)) {
            @Override
            public java.util.Optional<SerializedForm> getSerializedForm() {
                throw new AssertionError("Generic records must bypass serialized-form inspection");
            }
        };

        assertEquals("[{\"id\":42}]", writeRecord(record, schema));
    }

    @Test
    void testTimestampWithNullFormat() throws IOException {
        final Map<String, Object> values = new HashMap<>();
        values.put("timestamp", new Timestamp(37293723L));
        values.put("time", new Time(37293723L));
        final Date date = Date.valueOf("1970-01-01");
        values.put("date", date);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        fields.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(schema, values);
        final RecordSet rs = RecordSet.of(schema, record);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.write(rs);
        }

        final String expected = String.format("[{\"timestamp\":37293723,\"time\":37293723,\"date\":%d}]", date.getTime());

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testTimestampRepresentations() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType())));
        final Timestamp timestamp = new Timestamp(1623926285001L);
        final Record record = new MapRecord(schema, Map.of("timestamp", timestamp));

        assertEquals("[{\"timestamp\":\"formatted-001\"}]",
                writeTimestampRecord(record, "'formatted-'SSS", TimestampRepresentation.FORMATTED_STRING, false));
        assertEquals("[{\"timestamp\":1623926285001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_MILLISECONDS, false));
        assertEquals("[{\"timestamp\":1623926285.001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false));
    }

    @Test
    void testEpochSecondsValues() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType())));
        final long[] epochMilliseconds = {1623926285000L, 1623926285001L, 1623926285999L, 0L, -1L, 253402300799999L};
        final String[] expectedValues = {"1623926285.000", "1623926285.001", "1623926285.999", "0.000", "-0.001", "253402300799.999"};

        for (int i = 0; i < epochMilliseconds.length; i++) {
            final Record record = new MapRecord(schema, Map.of("timestamp", new Timestamp(epochMilliseconds[i])));
            assertEquals("[{\"timestamp\":" + expectedValues[i] + "}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false));
        }
    }

    @Test
    void testEpochSecondsRawRecord() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType())));
        final Record record = new MapRecord(schema, Map.of("timestamp", new Timestamp(1623926285001L)));

        assertEquals("[{\"timestamp\":1623926285.001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, true));
    }

    @Test
    void testEpochSecondsDoesNotChangeDateAndTime() throws IOException {
        final Date date = Date.valueOf("1970-01-01");
        final Time time = new Time(37293723L);
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("date", RecordFieldType.DATE.getDataType()),
                new RecordField("time", RecordFieldType.TIME.getDataType())));
        final Record record = new MapRecord(schema, Map.of("timestamp", new Timestamp(37293723L), "date", date, "time", time));

        assertEquals(String.format("[{\"timestamp\":37293.723,\"date\":%d,\"time\":37293723}]", date.getTime()),
                writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false));
    }

    @Test
    void testEpochSecondsNestedArrayAndChoice() throws IOException {
        final DataType timestampType = RecordFieldType.TIMESTAMP.getDataType();
        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(new RecordField("timestamp", timestampType)));
        final Record nestedRecord = new MapRecord(nestedSchema, Map.of("timestamp", new Timestamp(1623926285001L)));
        final List<RecordField> fields = List.of(
                new RecordField("nested", RecordFieldType.RECORD.getRecordDataType(nestedSchema)),
                new RecordField("timestamps", RecordFieldType.ARRAY.getArrayDataType(timestampType)),
                new RecordField("choice", RecordFieldType.CHOICE.getChoiceDataType(timestampType, RecordFieldType.STRING.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Record record = new MapRecord(schema, Map.of(
                "nested", nestedRecord,
                "timestamps", new Timestamp[]{new Timestamp(0L), new Timestamp(-1L)},
                "choice", new Timestamp(1623926285999L)));

        assertEquals("[{\"nested\":{\"timestamp\":1623926285.001},\"timestamps\":[0.000,-0.001],\"choice\":1623926285.999}]",
                writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false));
    }

    @Test
    void testExplicitTimestampRepresentationDisablesSerializedFormReuse() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType())));
        final Record record = new MapRecord(schema, Map.of("timestamp", new Timestamp(1623926285001L)),
                SerializedForm.of("{\"timestamp\":\"preserved\"}", "application/json"));

        assertEquals("[{\"timestamp\":\"formatted-001\"}]", writeTimestampRecord(record, "'formatted-'SSS", TimestampRepresentation.FORMATTED_STRING, false));
        assertEquals("[{\"timestamp\":1623926285001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_MILLISECONDS, false));
        assertEquals("[{\"timestamp\":1623926285.001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false));
        assertEquals("[{\"timestamp\":1623926285.001}]", writeTimestampRecord(record, null, TimestampRepresentation.EPOCH_SECONDS, false, true));
    }

    private String writeTimestampRecord(final Record record, final String timestampFormat, final TimestampRepresentation timestampRepresentation,
            final boolean rawRecord) throws IOException {
        return writeTimestampRecord(record, timestampFormat, timestampRepresentation, rawRecord, false);
    }

    private String writeTimestampRecord(final Record record, final String timestampFormat, final TimestampRepresentation timestampRepresentation,
            final boolean rawRecord, final boolean allowScientificNotation) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), record.getSchema(), new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, timestampFormat, "application/json", allowScientificNotation, true,
                timestampRepresentation)) {
            writer.beginRecordSet();
            if (rawRecord) {
                writer.writeRawRecord(record);
            } else {
                writer.writeRecord(record);
            }
            writer.finishRecordSet();
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    @Test
    void testExtraFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\"}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testExtraFieldInWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("name", "John");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\",\"name\":\"John\"}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testMissingFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\",\"name\":null}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testMissingFieldInWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\"}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testMissingAndExtraFieldInWriteRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("dob", "1/1/1970");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\",\"name\":null}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testMissingAndExtraFieldInWriteRawRecord() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        values.put("dob", "1/1/1970");
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRawRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"id\":\"1\",\"dob\":\"1/1/1970\"}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testNullSuppression() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", "1");
        final Record recordWithMissingName = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.write(recordWithMissingName);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\",\"name\":null}]", baos.toString(StandardCharsets.UTF_8));

        baos.reset();
        try (
                final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                        NullSuppression.ALWAYS_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.write(recordWithMissingName);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\"}]", baos.toString(StandardCharsets.UTF_8));

        baos.reset();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.SUPPRESS_MISSING, OutputGrouping.OUTPUT_ARRAY, null, null,
                null)) {
            writer.beginRecordSet();
            writer.write(recordWithMissingName);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\"}]", baos.toString(StandardCharsets.UTF_8));

        // set an explicit null value
        values.put("name", null);
        final Record recordWithNullValue = new MapRecord(schema, values);

        baos.reset();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.write(recordWithNullValue);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\",\"name\":null}]", baos.toString(StandardCharsets.UTF_8));

        baos.reset();
        try (
                final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                        NullSuppression.ALWAYS_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.write(recordWithNullValue);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\"}]", baos.toString(StandardCharsets.UTF_8));

        baos.reset();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.SUPPRESS_MISSING, OutputGrouping.OUTPUT_ARRAY, null, null,
                null)) {
            writer.beginRecordSet();
            writer.write(recordWithNullValue);
            writer.finishRecordSet();
        }

        assertEquals("[{\"id\":\"1\",\"name\":null}]", baos.toString(StandardCharsets.UTF_8));

    }

    @Test
    void testOnelineOutput() throws IOException {
        final Map<String, Object> values1 = new HashMap<>();
        values1.put("timestamp", new Timestamp(37293723L));
        values1.put("time", new Time(37293723L));

        final Date date = Date.valueOf("1970-01-01");
        values1.put("date", date);

        final List<RecordField> fields1 = new ArrayList<>();
        fields1.add(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        fields1.add(new RecordField("time", RecordFieldType.TIME.getDataType()));
        fields1.add(new RecordField("date", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields1);

        final Record record1 = new MapRecord(schema, values1);

        final Map<String, Object> values2 = new HashMap<>();
        values2.put("timestamp", new Timestamp(37293999L));
        values2.put("time", new Time(37293999L));
        values2.put("date", date);

        final Record record2 = new MapRecord(schema, values2);

        final RecordSet rs = RecordSet.of(schema, record1, record2);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ONELINE, null, null, null)) {
            writer.write(rs);
        }

        final long dateTime = date.getTime();
        final String expected = String.format("{\"timestamp\":37293723,\"time\":37293723,\"date\":%d}\n{\"timestamp\":37293999,\"time\":37293999,\"date\":%d}", dateTime, dateTime);

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testChoiceArray() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("path", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()))));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        Object[] paths = new Object[1];
        paths[0] = "10.2.1.3";

        final Map<String, Object> values = new HashMap<>();
        values.put("path", paths);
        final Record record = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRecord(record);
            writer.finishRecordSet();
        }

        final String expected = "[{\"path\":[\"10.2.1.3\"]}]";

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(expected, output);
    }

    @Test
    void testChoiceArrayOfStringsOrArrayOfRecords() throws IOException {
        final String jsonFirstItem = "{\"itemData\":[\"test\"]}";
        final String jsonSecondItem = "{\"itemData\":[{\"quantity\":10}]}";
        final String json = String.format("[{\"items\":[%s,%s]}]", jsonFirstItem, jsonSecondItem);

        final JsonSchemaInference jsonSchemaInference = new JsonSchemaInference(new TimeValueInference(null, null, null));
        final RecordSchema schema = jsonSchemaInference.inferSchema(new JsonRecordSource(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));

        final Map<String, Object> itemData1 = new HashMap<>();
        itemData1.put("itemData", new String[]{"test"});

        final Map<String, Object> quantityMap = new HashMap<>();
        quantityMap.put("quantity", 10);
        final List<RecordField> itemDataRecordFields = new ArrayList<>(1);
        itemDataRecordFields.add(new RecordField("quantity", RecordFieldType.INT.getDataType(), true));
        final RecordSchema quantityRecordSchema = new SimpleRecordSchema(itemDataRecordFields);
        final Record quantityRecord = new MapRecord(quantityRecordSchema, quantityMap);

        final Record[] quantityRecordArray = {quantityRecord};
        final Map<String, Object> itemData2 = new HashMap<>();

        itemData2.put("itemData", quantityRecordArray);

        final Object[] itemDataArray = {itemData1, itemData2};

        final Map<String, Object> values = new HashMap<>();
        values.put("items", itemDataArray);
        Record topLevelRecord = new MapRecord(schema, values);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, null, null, null)) {
            writer.beginRecordSet();
            writer.writeRecord(topLevelRecord);
            writer.finishRecordSet();
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals(json, output);
    }

    @Test
    void testGenericStringSerializationFallsBackToTypedValues() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        values.put("age", 42);

        final String rawForm = "{\"name\":\"John Doe\",\"age\":42,\"ignoredExtra\":\"preserved\"}";
        final SerializedForm serializedForm = SerializedForm.of(rawForm, "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.write(RecordSet.of(schema, record));
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertEquals("[{\"name\":\"John Doe\",\"age\":42}]", output);
    }

    @Test
    void testGenericByteArraySerializationFallsBackToTypedValues() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "Jöhn Doe");
        values.put("age", 42);

        final String rawForm = "{\"name\":\"Jöhn Doe\",\"age\":42,\"ignoredExtra\":\"preserved\"}";
        final SerializedForm serializedForm = SerializedForm.of(rawForm.getBytes(StandardCharsets.UTF_8), "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.write(RecordSet.of(schema, record));
        }

        assertEquals("[{\"name\":\"Jöhn Doe\",\"age\":42}]", baos.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testGenericSerializedFormCannotBypassSchemaCoercion() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final Record record = new MapRecord(schema, Map.of("id", 42),
                SerializedForm.of("{\"id\":\"42\"}", "application/json"));

        assertEquals("[{\"id\":42}]", writeRecord(record, schema, true));
    }

    @Test
    void testNonUtf8ByteArraySerializationFallsBack() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));

        for (final java.nio.charset.Charset charset : List.of(StandardCharsets.UTF_16BE, StandardCharsets.UTF_16LE,
                java.nio.charset.Charset.forName("UTF-32BE"), java.nio.charset.Charset.forName("UTF-32LE"))) {
            for (final String prefix : List.of("", "\uFEFF")) {
                final byte[] serialized = (prefix + "{\"id\":1}").getBytes(charset);
                final Record optimizedRecord = new MapRecord(schema, Map.of("id", 1), SerializedForm.of(serialized, "application/json"));
                final Record referenceRecord = new MapRecord(schema, Map.of("id", 1), SerializedForm.of(serialized, "application/json"));

                assertEquals(writeRecord(referenceRecord, schema, false), writeRecord(optimizedRecord, schema, true));
                assertEquals("[{\"id\":1}]", writeRecord(optimizedRecord, schema, true));
            }
        }
    }

    @Test
    void testValidatedSerializationAddsMissingFieldsWithoutMaterialization() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.LONG.getDataType())));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.LONG.getDataType()),
                new RecordField("optional", RecordFieldType.STRING.getDataType(), true)));
        final String rawForm = "{\"id\":1}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> {
                    throw new AssertionError("Compatible merged schema should not materialize the record");
                });

        assertEquals("[{\"id\":1,\"optional\":null}]", writeRecord(record, writeSchema));
        assertFalse(record.isMaterialized());
    }

    @Test
    void testIncorporateSchemaDisablesExactSchemaSerializedReuse() throws IOException {
        assertMaterializedOperationUsesTypedValues(record -> record.incorporateSchema(record.getSchema()));
    }

    @Test
    void testIncorporateInactiveFieldsDisablesExactSchemaSerializedReuse() throws IOException {
        assertMaterializedOperationUsesTypedValues(Record::incorporateInactiveFields);
    }

    @Test
    void testRegenerateSchemaDisablesExactSchemaSerializedReuse() throws IOException {
        assertMaterializedOperationUsesTypedValues(Record::regenerateSchema);
    }

    @Test
    void testRecordSchemaRenameDisablesSerializedReuse() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final DeferredJsonRecord record = deferredRecord(schema, new LinkedHashMap<>(Map.of("id", 1)), "{\"id\":999}");

        assertTrue(record.getSchema().renameField("id", "identifier"));

        assertEquals("[{\"identifier\":null}]", writeRecord(record, schema));
        assertTrue(record.isMaterialized());
    }

    @Test
    void testWriterSchemaMutationInvalidatesCachedCompatibility() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("optional", RecordFieldType.STRING.getDataType(), true)));
        final DeferredJsonRecord first = deferredRecord(sourceSchema, new LinkedHashMap<>(Map.of("id", 1)), "{\"id\":1}");
        final DeferredJsonRecord second = deferredRecord(sourceSchema, new LinkedHashMap<>(Map.of("id", 2)), "{\"id\":2}");
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        try (StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), writeSchema,
                new SchemaNameAsAttribute(), output, false, NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.beginRecordSet();
            writer.writeRawRecord(first);
            assertTrue(writeSchema.renameField("optional", "renamed"));
            writer.write(second);
            writer.finishRecordSet();
            writer.flush();
        }

        assertEquals("[{\"id\":1,\"optional\":null},{\"id\":2,\"renamed\":null}]", output.toString(StandardCharsets.UTF_8));
        assertFalse(first.isMaterialized());
        assertTrue(second.isMaterialized());
    }

    @Test
    void testNestedRecordSchemaRemovalDisablesSerializedReuse() throws IOException {
        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(
                new RecordField("status", RecordFieldType.STRING.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("nested", RecordFieldType.RECORD.getRecordDataType(nestedSchema))));
        final MapRecord nestedRecord = new MapRecord(nestedSchema, Map.of("status", "typed"));
        final DeferredJsonRecord record = deferredRecord(schema, new LinkedHashMap<>(Map.of("nested", nestedRecord)),
                "{\"nested\":{\"status\":\"stale\"}}");

        nestedSchema.removeField("status");

        assertEquals("[{\"nested\":{}}]", writeRecord(record, schema));
        assertTrue(record.isMaterialized());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMaterializedMapMutationDisablesExactSchemaSerializedReuse() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()))));
        final Map<String, Object> attributes = new LinkedHashMap<>(Map.of("status", "old"));
        final DeferredJsonRecord record = deferredRecord(schema, new LinkedHashMap<>(Map.of("attributes", attributes)),
                "{\"attributes\":{\"status\":\"old\"}}");

        ((Map<String, Object>) record.getValue("attributes")).put("status", "new");

        assertEquals("[{\"attributes\":{\"status\":\"new\"}}]", writeRecord(record, schema));
    }

    @Test
    void testMaterializedArrayMutationDisablesExactSchemaSerializedReuse() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("values", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()))));
        final Object[] values = {1, 2};
        final DeferredJsonRecord record = deferredRecord(schema, new LinkedHashMap<>(Map.of("values", values)),
                "{\"values\":[1,2]}");

        record.getAsArray("values")[1] = 3;

        assertEquals("[{\"values\":[1,3]}]", writeRecord(record, schema));
    }

    private void assertMaterializedOperationUsesTypedValues(final Consumer<Record> operation) throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final DeferredJsonRecord record = deferredRecord(schema, new LinkedHashMap<>(Map.of("id", 1)), "{\"id\":999}");

        operation.accept(record);

        assertTrue(record.isMaterialized());
        assertEquals("[{\"id\":1}]", writeRecord(record, record.getSchema()));
    }

    private DeferredJsonRecord deferredRecord(final RecordSchema schema, final Map<String, Object> values, final String rawForm) {
        final SerializedForm serializedForm = SerializedForm.of(
                new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json");
        return new DeferredJsonRecord(schema, true, false, serializedForm,
                () -> new MapRecord(schema, values, serializedForm));
    }

    @Test
    void testValidatedEmptyObjectAddsMissingFieldWithoutLeadingComma() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType(), true)));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType(), true),
                new RecordField("optional", RecordFieldType.STRING.getDataType(), true)));
        final String rawForm = "{}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8), 0, rawForm.length(), false, false), "application/json"),
                () -> {
                    throw new AssertionError("Compatible empty object should not materialize the record");
                });

        assertEquals("[{\"optional\":null}]", writeRecord(record, writeSchema));
        assertFalse(record.isMaterialized());
    }

    @Test
    void testValidatedWhitespaceOnlyObjectAddsMissingFieldWithoutLeadingComma() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType(), true)));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType(), true),
                new RecordField("optional", RecordFieldType.STRING.getDataType(), true)));
        final String rawForm = "{ \t }";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> {
                    throw new AssertionError("Compatible empty object should not materialize the record");
                });

        assertEquals("[{\"optional\":null}]", writeRecord(record, writeSchema));
        assertFalse(record.isMaterialized());
    }

    @Test
    void testObjectContentsPreservesKnownLineBreakMetadata() {
        final byte[] multiline = "{\n\"id\":1\n}".getBytes(StandardCharsets.UTF_8);
        final byte[] compact = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);

        assertTrue(new Utf8JsonValue(multiline, 0, multiline.length, true).objectContents().containsLineBreak());
        assertFalse(new Utf8JsonValue(compact, 0, compact.length, false).objectContents().containsLineBreak());
    }

    @Test
    void testValidatedSerializationMaterializesForMissingFieldDefault() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("status", RecordFieldType.STRING.getDataType(), "active", true)));
        final String rawForm = "{\"id\":1}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> new MapRecord(sourceSchema, Map.of("id", 1)));

        assertEquals("[{\"id\":1,\"status\":\"active\"}]", writeRecord(record, writeSchema));
        assertTrue(record.isMaterialized());
    }

    @Test
    void testValidatedSerializationSuppressesMissingFieldsWithoutMaterialization() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("first", RecordFieldType.STRING.getDataType(), true),
                new RecordField("second", RecordFieldType.BOOLEAN.getDataType(), true)));
        final String rawForm = "{\"id\":1}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> {
                    throw new AssertionError("Compatible merged schema should not materialize the record");
                });

        assertEquals("[" + rawForm + "]", writeRecord(record, writeSchema, NullSuppression.SUPPRESS_MISSING));
        assertFalse(record.isMaterialized());
    }

    @Test
    void testValidatedSerializationMaterializesForNestedSchemaEvolution() throws IOException {
        final RecordSchema sourceNestedSchema = new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType())));
        final RecordSchema writeNestedSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("optional", RecordFieldType.STRING.getDataType(), true)));
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(
                new RecordField("nested", RecordFieldType.RECORD.getRecordDataType(sourceNestedSchema))));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("nested", RecordFieldType.RECORD.getRecordDataType(writeNestedSchema))));
        final String rawForm = "{\"nested\":{\"id\":1}}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> new MapRecord(sourceSchema, Map.of("nested", new MapRecord(sourceNestedSchema, Map.of("id", 1)))));

        assertEquals("[{\"nested\":{\"id\":1,\"optional\":null}}]", writeRecord(record, writeSchema));
        assertTrue(record.isMaterialized());
    }

    @Test
    void testValidatedSerializationMaterializesForIncompatibleWriterSchema() throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("legacy", RecordFieldType.BOOLEAN.getDataType())));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final String rawForm = "{\"id\":1,\"legacy\":true}";
        final DeferredJsonRecord record = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> new MapRecord(sourceSchema, Map.of("id", 1, "legacy", true)));

        assertEquals("[{\"id\":1}]", writeRecord(record, writeSchema));
        assertTrue(record.isMaterialized());
    }

    @Test
    void testValidatedSerializationMaterializesForNumericRepresentationChanges() throws IOException {
        assertNumericWideningFallsBack(RecordFieldType.INT.getDataType(), 0,
                RecordFieldType.LONG.getDataType(), "{\"value\" : -0}");
        assertNumericWideningFallsBack(RecordFieldType.INT.getDataType(), 1,
                RecordFieldType.DOUBLE.getDataType(), "{\"value\":1}");
        assertNumericWideningFallsBack(RecordFieldType.LONG.getDataType(), 9_007_199_254_740_993L,
                RecordFieldType.DOUBLE.getDataType(), "{\"value\":9007199254740993}");
        assertNumericWideningFallsBack(RecordFieldType.DOUBLE.getDataType(), 1.25D,
                RecordFieldType.DECIMAL.getDecimalDataType(10, 2), "{\"value\":1.25}");
    }

    private void assertNumericWideningFallsBack(final DataType sourceType, final Number value,
                                                final DataType writeType, final String rawForm) throws IOException {
        final RecordSchema sourceSchema = new SimpleRecordSchema(List.of(new RecordField("value", sourceType)));
        final RecordSchema writeSchema = new SimpleRecordSchema(List.of(new RecordField("value", writeType)));
        final DeferredJsonRecord optimizedRecord = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> new MapRecord(sourceSchema, Map.of("value", value)));
        final DeferredJsonRecord referenceRecord = new DeferredJsonRecord(sourceSchema, true, false,
                SerializedForm.of(new Utf8JsonValue(rawForm.getBytes(StandardCharsets.UTF_8)), "application/json"),
                () -> new MapRecord(sourceSchema, Map.of("value", value)));

        assertEquals(writeRecord(referenceRecord, writeSchema, false), writeRecord(optimizedRecord, writeSchema, true));
        assertTrue(optimizedRecord.isMaterialized());
    }

    private String writeRecord(final Record record, final RecordSchema writeSchema) throws IOException {
        return writeRecord(record, writeSchema, NullSuppression.NEVER_SUPPRESS);
    }

    private String writeRecord(final Record record, final RecordSchema writeSchema, final NullSuppression nullSuppression) throws IOException {
        return writeRecord(record, writeSchema, nullSuppression, true);
    }

    private String writeRecord(final Record record, final RecordSchema writeSchema,
                               final boolean serializedInputHandlingEnabled) throws IOException {
        return writeRecord(record, writeSchema, NullSuppression.NEVER_SUPPRESS, serializedInputHandlingEnabled);
    }

    private String writeRecord(final Record record, final RecordSchema writeSchema, final NullSuppression nullSuppression,
                               final boolean serializedInputHandlingEnabled) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), writeSchema,
                new SchemaNameAsAttribute(), output, false, nullSuppression, OutputGrouping.OUTPUT_ARRAY,
                RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(),
                RecordFieldType.TIMESTAMP.getDefaultFormat(), "application/json", false, serializedInputHandlingEnabled)) {
            writer.write(RecordSet.of(writeSchema, record));
        }
        return output.toString(StandardCharsets.UTF_8);
    }

    @Test
    void testCarriageReturnByteArraySerializationUsesCompactFallback() throws IOException {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType())));
        final Map<String, Object> values = Map.of("name", "John Doe", "age", 42);
        final String rawForm = "{\r\"name\":\"John Doe\",\"age\":42,\"ignoredExtra\":\"removed\"}";
        final Record record = new MapRecord(schema, values, SerializedForm.of(rawForm.getBytes(StandardCharsets.UTF_8), "application/json"));

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), output, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.write(RecordSet.of(schema, record));
        }

        assertEquals("[{\"name\":\"John Doe\",\"age\":42}]", output.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testGenericPrettyByteArraySerializationFallsBackToTypedValues() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = Map.of("name", "John Doe", "age", 42);
        final String rawForm = """
                {
                  "name": "John Doe",
                  "age": 42,
                  "ignoredExtra": "preserved"
                }""";
        final SerializedForm serializedForm = SerializedForm.of(rawForm.getBytes(StandardCharsets.UTF_8), "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, true,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat())) {
            writer.write(RecordSet.of(schema, record));
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertFalse(output.contains("ignoredExtra"));
        assertTrue(output.contains("\"name\" : \"John Doe\""));
        assertTrue(output.contains("\"age\" : 42"));
    }

    @Test
    void testReuseInputSerializationFalseForcesReserialization() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("age", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        values.put("age", 42);

        final String rawForm = "{\"name\":\"John Doe\",\"age\":42,\"ignoredExtra\":\"preserved\"}";
        final SerializedForm serializedForm = SerializedForm.of(rawForm, "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(),
                "application/json", false, false)) {
            writer.write(RecordSet.of(schema, record));
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertFalse(output.contains("ignoredExtra"),
                "When Serialized JSON Input Handling is DISABLED, the writer must re-serialize from typed values and ignore raw bytes");
        assertEquals("[{\"name\":\"John Doe\",\"age\":42}]", output);
    }

    @Test
    void testReuseInputSerializationFalseHonorsTimestampFormat() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("event", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Timestamp eventTimestamp = Timestamp.valueOf("2025-03-20 17:33:11.000");
        final Map<String, Object> values = new HashMap<>();
        values.put("event", eventTimestamp);

        final String timestampValue = "2025-03-20T17:33:11.000+0000";
        final String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
        final String rawForm = "{\"event\":\"%s\"}".formatted(timestampValue);
        final SerializedForm serializedForm = SerializedForm.of(rawForm, "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream fastPathBaos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), fastPathBaos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), timestampFormat,
                "application/json", false, true)) {
            writer.write(RecordSet.of(schema, record));
        }

        assertFalse(fastPathBaos.toString(StandardCharsets.UTF_8).contains(timestampValue),
                "Generic serialized forms must use typed output even when Serialized JSON Input Handling is enabled");

        final ByteArrayOutputStream slowPathBaos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), slowPathBaos, false,
                NullSuppression.NEVER_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), timestampFormat,
                "application/json", false, false)) {
            writer.write(RecordSet.of(schema, record));
        }

        final String slowPathOutput = slowPathBaos.toString(StandardCharsets.UTF_8);
        assertFalse(slowPathOutput.contains("+0000"),
                "With Serialized JSON Input Handling DISABLED, writer's Timestamp Format must be applied even when SerializedForm is present");
        assertTrue(slowPathOutput.contains("\"event\":\"2025-03-20T17:33:11.000"),
                "Re-serialized timestamp should reflect the configured format");
    }

    @Test
    void testReuseInputSerializationFalseHonorsSuppressNulls() throws IOException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("middleName", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        values.put("middleName", null);

        final String rawForm = "{\"name\":\"John Doe\",\"middleName\":null}";
        final SerializedForm serializedForm = SerializedForm.of(rawForm, "application/json");
        final Record record = new MapRecord(schema, values, serializedForm);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final StreamingJsonWriteResult writer = new StreamingJsonWriteResult(Mockito.mock(ComponentLog.class), schema, new SchemaNameAsAttribute(), baos, false,
                NullSuppression.ALWAYS_SUPPRESS, OutputGrouping.OUTPUT_ARRAY, RecordFieldType.DATE.getDefaultFormat(),
                RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat(),
                "application/json", false, false)) {
            writer.write(RecordSet.of(schema, record));
        }

        final String output = baos.toString(StandardCharsets.UTF_8);
        assertFalse(output.contains("middleName"),
                "Suppress Null Values must be honored when Serialized JSON Input Handling is DISABLED, even though the input JSON contained the null field");
        assertEquals("[{\"name\":\"John Doe\"}]", output);
    }
}
