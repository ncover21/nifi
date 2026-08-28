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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class RecordStreamKafkaMessageConverterTest {

    private final RecordSetWriterFactory writerFactory = mock(RecordSetWriterFactory.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
    private final ComponentLog logger = mock(ComponentLog.class);
    private final ProcessSession session = mock(ProcessSession.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));

    private final OffsetTracker offsetTracker = new OffsetTracker();

    @Test
    void testGroupingOfMessagesByTopicAndPartition() throws Exception {
        // Initialize MockRecordParser
        final MockRecordParser readerFactory = spy(new MockRecordParser());
        readerFactory.addSchemaField("field1", RecordFieldType.STRING);

        // Add records to MockRecordParser
        readerFactory.addRecord("value1");
        readerFactory.addRecord("value2");
        readerFactory.addRecord("value3");
        readerFactory.addRecord("value4");

        // Initialize the converter
        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory,
                writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8,
                true,
                offsetTracker,
                logger,
                "brokerUri",
                new CreateNewFlowFileGrouping(writerFactory, logger, "brokerUri", true)
        );

        // Create ByteRecords
        final ByteRecord group1Record1 = new ByteRecord("topic1", 0, 0, 1000L, List.of(), null, "value1".getBytes(), 0L);
        final ByteRecord group1Record2 = new ByteRecord("topic1", 0, 3, 500L, List.of(), null, "value4".getBytes(), 0L);

        final ByteRecord group2 = new ByteRecord("topic1", 1, 1, 2000L, List.of(), null, "value2".getBytes(), 0L);

        final ByteRecord group3 = new ByteRecord("topic2", 0, 2, 3000L, List.of(), null, "value3".getBytes(), 0L);

        final Iterator<ByteRecord> consumerRecords = List.of(group1Record1, group2, group3, group1Record2).iterator();
        // Mock the session.create() and session.write() methods
        final FlowFile flowFile1 = new MockFlowFile(1);
        final FlowFile flowFile2 = new MockFlowFile(2);
        final FlowFile flowFile3 = new MockFlowFile(3);
        final FlowFile flowFile4 = new MockFlowFile(4);
        when(session.create()).thenReturn(flowFile1, flowFile2, flowFile3, flowFile4);
        when(session.write(any(FlowFile.class))).thenReturn(mock(OutputStream.class));

        // Call the method under test
        converter.toFlowFiles(session, consumerRecords);

        verify(readerFactory, times(4)).createRecordReader(anyMap(), any(InputStream.class), anyLong(), same(logger));

        // Verify that the messages are grouped correctly
        final ArgumentCaptor<Map<String, String>> attributesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(session, atLeastOnce()).putAllAttributes(any(FlowFile.class), attributesCaptor.capture());

        final List<Map<String, String>> capturedAttributes = attributesCaptor.getAllValues();

        // check group1 records
        assertEquals("topic1", capturedAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        assertEquals("topic1", capturedAttributes.get(3).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(3).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        //check group2 records
        assertEquals("topic1", capturedAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("1", capturedAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        //check group3 records
        assertEquals("topic2", capturedAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        final List<String> timestamps = capturedAttributes.stream()
                .map(attrs -> attrs.get(KafkaFlowFileAttribute.KAFKA_TIMESTAMP))
                .filter(Objects::nonNull)
                .toList();
        assertTrue(timestamps.contains("500"), "Expected timestamp from group1Record2");
        assertTrue(timestamps.contains("2000"), "Expected timestamp from group2");
        assertTrue(timestamps.contains("3000"), "Expected timestamp from group3");
    }

    @Test
    void testMalformedTrailingRecordPreservesEarlierGroupingAndRoutesFailure() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record firstRecord = new MapRecord(schema, Map.of("value", "first"));

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.RETAINABLE);
        when(reader.nextRecord()).thenReturn(firstRecord).thenThrow(new MalformedRecordException("Malformed trailing record"));
        final FlowFile parseFailure = new MockFlowFile(1);
        final ByteArrayOutputStream failureContent = new ByteArrayOutputStream();
        when(session.create()).thenReturn(parseFailure);
        when(session.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
        when(session.write(same(parseFailure), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            invocation.getArgument(1, OutputStreamCallback.class).process(failureContent);
            return parseFailure;
        });

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        verify(groupingStrategy).addRecord(any(), any(), same(firstRecord), any(), anyMap(), anyMap());
        verify(groupingStrategy).finishAllGroups(session);
        verify(session).transfer(parseFailure, org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE);
        assertEquals("value", failureContent.toString(StandardCharsets.UTF_8));
        assertEquals(5, offsetTracker.getTotalRecordSize());
    }

    @Test
    void testReusableRecordIsConsumedBeforeReaderAdvances() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final Record record = mock(Record.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final AtomicInteger recordIndex = new AtomicInteger(-1);
        final List<String> groupedValues = new ArrayList<>();

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.STREAMING);
        when(reader.nextRecord()).thenAnswer(ignored -> recordIndex.incrementAndGet() < 2 ? record : null);
        when(record.getSchema()).thenReturn(schema);
        when(record.getValue("value")).thenAnswer(ignored -> recordIndex.get() == 0 ? "first" : "second");
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        doAnswer(invocation -> {
            groupedValues.add(invocation.getArgument(2, Record.class).getValue("value").toString());
            return null;
        }).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        assertEquals(List.of("first", "second"), groupedValues);
        verify(groupingStrategy).finishAllGroups(session);
    }

    @Test
    void testReusableRecordIsRetainedForBufferingGroupingStrategy() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final Record record = mock(Record.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final AtomicInteger recordIndex = new AtomicInteger(-1);
        final List<Record> groupedRecords = new ArrayList<>();

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.STREAMING);
        when(reader.nextRecord()).thenAnswer(ignored -> recordIndex.incrementAndGet() < 2 ? record : null);
        when(record.getSchema()).thenReturn(schema);
        when(record.toMap()).thenAnswer(ignored -> Map.of("value", recordIndex.get() == 0 ? "first" : "second"));
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        when(groupingStrategy.isRecordRetentionRequired()).thenReturn(true);
        doAnswer(invocation -> {
            groupedRecords.add(invocation.getArgument(2, Record.class));
            return null;
        }).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        assertEquals(List.of("first", "second"), groupedRecords.stream().map(grouped -> grouped.getValue("value")).toList());
    }

    @Test
    void testStreamingRetentionCopiesPrimitiveAndConcreteRecordArrays() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final Record record = mock(Record.class);
        final Record nestedRecord = mock(Record.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema nestedSchema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("bytes", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())),
                new RecordField("integers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())),
                new RecordField("strings", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("records", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(nestedSchema))),
                new RecordField("nested", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(nestedSchema)))));
        final byte[] bytes = {1, 2, 3};
        final int[] integers = {4, 5, 6};
        final String[] strings = {"one", "two"};
        final Object[] concreteRecords = (Object[]) Array.newInstance(nestedRecord.getClass(), 1);
        concreteRecords[0] = nestedRecord;
        final Object[] nested = {concreteRecords, new Object[]{nestedRecord}};
        final List<Record> groupedRecords = new ArrayList<>();

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.STREAMING);
        when(reader.nextRecord()).thenReturn(record).thenReturn(null);
        when(record.getSchema()).thenReturn(schema);
        when(record.toMap()).thenReturn(Map.of("bytes", bytes, "integers", integers, "strings", strings, "records", concreteRecords, "nested", nested));
        when(nestedRecord.getSchema()).thenReturn(nestedSchema);
        when(nestedRecord.toMap()).thenReturn(Map.of("value", "retained"));
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        when(groupingStrategy.isRecordRetentionRequired()).thenReturn(true);
        doAnswer(invocation -> {
            groupedRecords.add(invocation.getArgument(2, Record.class));
            return null;
        }).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        converter.toFlowFiles(session, List.of(new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L)).iterator());

        final Record retained = groupedRecords.getFirst();
        final byte[] retainedBytes = (byte[]) retained.getValue("bytes");
        final int[] retainedIntegers = (int[]) retained.getValue("integers");
        assertNotSame(bytes, retainedBytes);
        assertNotSame(integers, retainedIntegers);
        assertArrayEquals(bytes, retainedBytes);
        assertArrayEquals(integers, retainedIntegers);
        final String[] retainedStrings = (String[]) retained.getValue("strings");
        assertNotSame(strings, retainedStrings);
        assertArrayEquals(strings, retainedStrings);

        final Object[] retainedRecords = (Object[]) retained.getValue("records");
        assertSame(Object[].class, retainedRecords.getClass());
        assertInstanceOf(MapRecord.class, retainedRecords[0]);
        assertEquals("retained", ((Record) retainedRecords[0]).getValue("value"));
        final Object[] retainedNested = (Object[]) retained.getValue("nested");
        assertInstanceOf(MapRecord.class, ((Object[]) retainedNested[0])[0]);
        assertInstanceOf(MapRecord.class, ((Object[]) retainedNested[1])[0]);
    }

    @Test
    void testRuntimeFailureAbortsAllGroups() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));
        final ProcessException failure = new ProcessException("Grouping failed");

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.RETAINABLE);
        when(reader.nextRecord()).thenReturn(record).thenReturn(null);
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        doThrow(failure).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        assertSame(failure, assertThrows(ProcessException.class, () -> converter.toFlowFiles(session, List.of(input).iterator())));
        verify(groupingStrategy).abortAllGroups();
        verify(groupingStrategy, never()).finishAllGroups(any());
    }

    @Test
    void testProcessingAndAbortSameErrorPreservesOriginal() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));
        final AssertionError failure = new AssertionError("grouping and abort failed");

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.RETAINABLE);
        when(reader.nextRecord()).thenReturn(record).thenReturn(null);
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        doThrow(failure).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
        doThrow(failure).when(groupingStrategy).abortAllGroups();

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        assertSame(failure, assertThrows(AssertionError.class, () -> converter.toFlowFiles(session, List.of(input).iterator())));
        assertEquals(0, failure.getSuppressed().length);
        verify(groupingStrategy).abortAllGroups();
        verify(groupingStrategy, never()).finishAllGroups(any());
    }

    @Test
    void testLargeRetainableMessageStreamsAsReadAndResolvesSchemaOnce() throws Exception {
        final int recordCount = 10_000;
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));
        final AtomicInteger validationCount = new AtomicInteger();
        final AtomicInteger groupedCount = new AtomicInteger();

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.RETAINABLE);
        when(reader.nextRecord()).thenAnswer(ignored -> {
            if (validationCount.getAndIncrement() < recordCount) {
                return record;
            }
            return null;
        });
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        doAnswer(ignored -> {
            assertTrue(validationCount.get() <= groupedCount.get() + 2);
            groupedCount.incrementAndGet();
            return null;
        }).when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        assertEquals(recordCount, groupedCount.get());
        verify(readerFactory).createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger));
        verify(writerFactory).getSchema(anyMap(), same(schema));
        verify(groupingStrategy).finishAllGroups(session);
    }

    @Test
    void testMalformedLargeMessageGroupsRecordsBeforeFailure() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader validationReader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));
        final AtomicInteger validationCount = new AtomicInteger();

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(validationReader);
        when(validationReader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.RETAINABLE);
        when(validationReader.nextRecord()).thenAnswer(ignored -> {
            if (validationCount.getAndIncrement() <= 10_000) {
                return record;
            }
            throw new MalformedRecordException("Malformed trailing record");
        });
        final FlowFile parseFailure = new MockFlowFile(1);
        when(session.create()).thenReturn(parseFailure);
        when(session.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
        when(session.write(same(parseFailure), any(OutputStreamCallback.class))).thenReturn(parseFailure);

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        verify(readerFactory).createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger));
        verify(groupingStrategy, times(10_001)).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
        verify(groupingStrategy).finishAllGroups(session);
    }

    @Test
    void testStructurallyEqualSchemaInstancesAreResolvedIndependently() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordField field = new RecordField("value", RecordFieldType.STRING.getDataType());
        final RecordSchema firstReadSchema = new SimpleRecordSchema(List.of(field));
        final RecordSchema secondReadSchema = new SimpleRecordSchema(List.of(field));
        final RecordSchema firstWriteSchema = mock(RecordSchema.class);
        final RecordSchema secondWriteSchema = mock(RecordSchema.class);

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.STREAMING);
        when(reader.nextRecord())
                .thenReturn(new MapRecord(firstReadSchema, Map.of("value", "first")))
                .thenReturn(new MapRecord(secondReadSchema, Map.of("value", "second")))
                .thenReturn(null);
        when(writerFactory.getSchema(anyMap(), same(firstReadSchema))).thenReturn(firstWriteSchema);
        when(writerFactory.getSchema(anyMap(), same(secondReadSchema))).thenReturn(secondWriteSchema);

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, new byte[0], 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        final ArgumentCaptor<RecordSchema> writeSchemas = ArgumentCaptor.forClass(RecordSchema.class);
        verify(groupingStrategy, times(2)).addRecord(any(), any(), any(), writeSchemas.capture(), anyMap(), anyMap());
        assertSame(firstWriteSchema, writeSchemas.getAllValues().get(0));
        assertSame(secondWriteSchema, writeSchemas.getAllValues().get(1));
    }

    @Test
    void testReaderCloseFailureRoutesMessageToParseFailureForEveryMode() throws Exception {
        for (final RecordReader.RecordHandlingMode handlingMode : RecordReader.RecordHandlingMode.values()) {
            final ByteArrayRecordReaderFactory testReaderFactory = mock(ByteArrayRecordReaderFactory.class);
            final RecordReader reader = mock(RecordReader.class);
            final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
            final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
            final ProcessSession testSession = mock(ProcessSession.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
            final OffsetTracker testOffsetTracker = new OffsetTracker();
            final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
            final Record record = new MapRecord(schema, Map.of("value", "record"));

            when(testReaderFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
            when(reader.getRecordHandlingMode()).thenReturn(handlingMode);
            when(reader.nextRecord()).thenReturn(record).thenReturn(null);
            doThrow(new IOException("Close failed")).when(reader).close();
            when(testWriterFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);

            final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                    testReaderFactory, testWriterFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"),
                    KeyEncoding.UTF8, true, testOffsetTracker, logger, "brokerUri", groupingStrategy);
            final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

            converter.toFlowFiles(testSession, List.of(input).iterator());

            verify(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
            verify(groupingStrategy).finishAllGroups(testSession);
            verify(testSession).transfer(any(FlowFile.class), same(org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE));
            assertEquals(5, testOffsetTracker.getTotalRecordSize());
        }
    }

    @Test
    void testWriterSchemaFailureRoutesToParseFailureForEveryReaderMode() throws Exception {
        for (final RecordReader.RecordHandlingMode handlingMode : RecordReader.RecordHandlingMode.values()) {
            final ByteArrayRecordReaderFactory testReaderFactory = mock(ByteArrayRecordReaderFactory.class);
            final RecordReader reader = mock(RecordReader.class);
            final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
            final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
            final ProcessSession testSession = mock(ProcessSession.class);
            final OffsetTracker testOffsetTracker = new OffsetTracker();
            final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
            final Record record = new MapRecord(schema, Map.of("value", "record"));

            when(testReaderFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
            when(reader.getRecordHandlingMode()).thenReturn(handlingMode);
            when(reader.nextRecord()).thenReturn(record).thenReturn(null);
            when(testWriterFactory.getSchema(anyMap(), same(schema))).thenThrow(new IOException("Schema failed"));
            final FlowFile parseFailure = new MockFlowFile(1);
            when(testSession.create()).thenReturn(parseFailure);
            when(testSession.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
            when(testSession.write(same(parseFailure), any(OutputStreamCallback.class))).thenReturn(parseFailure);

            final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                    testReaderFactory, testWriterFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"),
                    KeyEncoding.UTF8, true, testOffsetTracker, logger, "brokerUri", groupingStrategy);
            final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

            converter.toFlowFiles(testSession, List.of(input).iterator());

            verify(groupingStrategy, never()).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
            verify(groupingStrategy).finishAllGroups(testSession);
            verify(testSession).transfer(parseFailure, org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE);
            assertEquals(5, testOffsetTracker.getTotalRecordSize());
        }
    }

    @Test
    void testWriterInitializationFailureRemovesOutputBeforeRoutingParseFailure() throws Exception {
        final ByteArrayRecordReaderFactory testReaderFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writer = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final OffsetTracker testOffsetTracker = new OffsetTracker();
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));
        final FlowFile abandonedOutput = new MockFlowFile(1);
        final FlowFile parseFailure = new MockFlowFile(2);
        final OutputStream output = mock(OutputStream.class);

        when(testReaderFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.nextRecord()).thenReturn(record).thenReturn(null);
        when(testWriterFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        when(testSession.create()).thenReturn(abandonedOutput, parseFailure);
        when(testSession.putAllAttributes(same(abandonedOutput), anyMap())).thenReturn(abandonedOutput);
        when(testSession.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
        when(testSession.write(abandonedOutput)).thenReturn(output);
        when(testSession.write(same(parseFailure), any(OutputStreamCallback.class))).thenReturn(parseFailure);
        when(testWriterFactory.createWriter(same(logger), same(schema), same(output), anyMap())).thenReturn(writer);
        doThrow(new IOException("Writer initialization failed")).when(writer).beginRecordSet();

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                testReaderFactory, testWriterFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"),
                KeyEncoding.UTF8, true, testOffsetTracker, logger, "brokerUri",
                new CreateNewFlowFileGrouping(testWriterFactory, logger, "brokerUri", true));
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

        converter.toFlowFiles(testSession, List.of(input).iterator());

        verify(writer).close();
        verify(output).close();
        verify(testSession).remove(abandonedOutput);
        verify(testSession).transfer(parseFailure, org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE);
        assertEquals(5, testOffsetTracker.getTotalRecordSize());
    }

    @Test
    void testRecordConversionFailureRoutesToParseFailureForEveryReaderMode() throws Exception {
        for (final RecordReader.RecordHandlingMode handlingMode : RecordReader.RecordHandlingMode.values()) {
            final ByteArrayRecordReaderFactory testReaderFactory = mock(ByteArrayRecordReaderFactory.class);
            final RecordReader reader = mock(RecordReader.class);
            final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
            final ProcessSession testSession = mock(ProcessSession.class);
            final OffsetTracker testOffsetTracker = new OffsetTracker();
            final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
            final Record record = new MapRecord(schema, Map.of("value", "record"));

            when(testReaderFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
            when(reader.getRecordHandlingMode()).thenReturn(handlingMode);
            when(reader.nextRecord()).thenReturn(record).thenReturn(null);
            final FlowFile parseFailure = new MockFlowFile(1);
            when(testSession.create()).thenReturn(parseFailure);
            when(testSession.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
            when(testSession.write(same(parseFailure), any(OutputStreamCallback.class))).thenReturn(parseFailure);

            final AbstractRecordStreamKafkaMessageConverter converter = new AbstractRecordStreamKafkaMessageConverter(
                    testReaderFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"),
                    KeyEncoding.UTF8, true, testOffsetTracker, logger, "brokerUri", groupingStrategy) {
                @Override
                protected RecordSchema getWriteSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord,
                                                      final Map<String, String> attributes) {
                    return inputSchema;
                }

                @Override
                protected Record convertRecord(final ByteRecord consumerRecord, final Record inputRecord,
                                               final Map<String, String> attributes) throws IOException {
                    throw new IOException("Conversion failed");
                }
            };
            final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

            converter.toFlowFiles(testSession, List.of(input).iterator());

            verify(groupingStrategy, never()).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
            verify(groupingStrategy).finishAllGroups(testSession);
            verify(testSession).transfer(parseFailure, org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE);
            assertEquals(5, testOffsetTracker.getTotalRecordSize());
        }
    }

    @Test
    void testGroupingFailureRoutesMessageToParseFailureAndAdvancesOffset() throws Exception {
        final ByteArrayRecordReaderFactory readerFactory = mock(ByteArrayRecordReaderFactory.class);
        final RecordReader reader = mock(RecordReader.class);
        final RecordGroupingStrategy groupingStrategy = mock(RecordGroupingStrategy.class);
        final RecordSchema schema = new SimpleRecordSchema(List.of(new RecordField("value", RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("value", "record"));

        when(readerFactory.createRecordReaderFromBytes(anyMap(), any(byte[].class), same(logger))).thenReturn(reader);
        when(reader.getRecordHandlingMode()).thenReturn(RecordReader.RecordHandlingMode.STREAMING);
        when(reader.nextRecord()).thenReturn(record).thenReturn(record).thenReturn(null);
        when(writerFactory.getSchema(anyMap(), same(schema))).thenReturn(schema);
        doAnswer(invocation -> null).doThrow(new IOException("Writer failed"))
                .when(groupingStrategy).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
        final FlowFile parseFailure = new MockFlowFile(1);
        when(session.create()).thenReturn(parseFailure);
        when(session.putAllAttributes(same(parseFailure), anyMap())).thenReturn(parseFailure);
        when(session.write(same(parseFailure), any(OutputStreamCallback.class))).thenReturn(parseFailure);

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory, writerFactory, value -> new String(value, StandardCharsets.UTF_8), Pattern.compile(".*"), KeyEncoding.UTF8,
                true, offsetTracker, logger, "brokerUri", groupingStrategy);
        final ByteRecord input = new ByteRecord("topic", 0, 1, 1000L, List.of(), null, "value".getBytes(StandardCharsets.UTF_8), 0L);

        converter.toFlowFiles(session, List.of(input).iterator());

        verify(groupingStrategy, times(2)).addRecord(any(), any(), any(), any(), anyMap(), anyMap());
        verify(groupingStrategy).finishAllGroups(session);
        verify(session).transfer(parseFailure, org.apache.nifi.kafka.processors.ConsumeKafka.PARSE_FAILURE);
        assertEquals(5, offsetTracker.getTotalRecordSize());
    }
}
