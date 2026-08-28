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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CreateNewFlowFileGroupingTest {

    private static final String TOPIC = "topic1";
    private static final String BROKER_URI = "brokerUri";

    private static final RecordSchema SCHEMA_A = new SimpleRecordSchema(List.of(
            new RecordField("fieldA", RecordFieldType.STRING.getDataType())));

    private static final RecordSchema SCHEMA_B = new SimpleRecordSchema(List.of(
            new RecordField("fieldB", RecordFieldType.INT.getDataType())));

    private static final Record RECORD_A = new MapRecord(SCHEMA_A, Map.of("fieldA", "hello"));
    private static final Record RECORD_B = new MapRecord(SCHEMA_B, Map.of("fieldB", 42));

    private final PassThroughSchemaRecordWriter writerFactory = new PassThroughSchemaRecordWriter();

    private MockProcessSession session;
    private ComponentLog logger;
    private CreateNewFlowFileGrouping grouping;

    @BeforeEach
    void setUp() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConsumeKafka.class);
        runner.addControllerService("writer", writerFactory);
        runner.enableControllerService(writerFactory);

        final Processor processor = runner.getProcessor();
        session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0)), processor);
        logger = runner.getLogger();
        grouping = new CreateNewFlowFileGrouping(writerFactory, logger, BROKER_URI, true);
    }

    @Test
    void testSameTopicPartitionAndSchemaShareOneFlowFile() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 10, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 11, 500L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.addRecord(session, second, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(1, success.size());

        final MockFlowFile flowFile = success.getFirst();
        assertEquals(TOPIC, flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION));
        assertEquals("10", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_OFFSET));
        assertEquals("11", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET));
        assertEquals("500", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_TIMESTAMP));
        assertEquals("2", flowFile.getAttribute("record.count"));
        assertEquals("true", flowFile.getAttribute(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED));
    }

    @Test
    void testDifferentWriteSchemasProduceSeparateFlowFiles() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 1, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 2, 2000L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.addRecord(session, second, RECORD_B, SCHEMA_B, Map.of(), Map.of());
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, success.size());
        assertTrue(success.stream().anyMatch(ff -> "1".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_OFFSET))));
        assertTrue(success.stream().anyMatch(ff -> "2".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_OFFSET))));
    }

    @Test
    void testDifferentGroupingAttributesProduceSeparateFlowFiles() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 1, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 0, 2, 2000L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of("hdr", "a"));
        grouping.addRecord(session, second, RECORD_A, SCHEMA_A, Map.of(), Map.of("hdr", "b"));
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, success.size());
        assertTrue(success.stream().anyMatch(ff -> "a".equals(ff.getAttribute("hdr"))));
        assertTrue(success.stream().anyMatch(ff -> "b".equals(ff.getAttribute("hdr"))));
    }

    @Test
    void testDifferentPartitionsProduceSeparateFlowFiles() throws Exception {
        final ByteRecord first = new ByteRecord(TOPIC, 0, 1, 1000L, List.of(), null, new byte[0], 0L);
        final ByteRecord second = new ByteRecord(TOPIC, 1, 2, 2000L, List.of(), null, new byte[0], 0L);

        grouping.addRecord(session, first, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.addRecord(session, second, RECORD_A, SCHEMA_A, Map.of(), Map.of());
        grouping.finishAllGroups(session);

        final List<MockFlowFile> success = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(2, success.size());
        assertTrue(success.stream().anyMatch(ff -> "0".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION))));
        assertTrue(success.stream().anyMatch(ff -> "1".equals(ff.getAttribute(KafkaFlowFileAttribute.KAFKA_PARTITION))));
    }

    @Test
    void testAbortClosesEveryOpenWriterAndClearsGroups() throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writerA = mock(RecordSetWriter.class);
        final RecordSetWriter writerB = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFileA = new MockFlowFile(1);
        final FlowFile flowFileB = new MockFlowFile(2);
        final OutputStream outputA = mock(OutputStream.class);
        final OutputStream outputB = mock(OutputStream.class);
        when(testSession.create()).thenReturn(flowFileA, flowFileB);
        when(testSession.putAllAttributes(same(flowFileA), anyMap())).thenReturn(flowFileA);
        when(testSession.putAllAttributes(same(flowFileB), anyMap())).thenReturn(flowFileB);
        when(testSession.write(flowFileA)).thenReturn(outputA);
        when(testSession.write(flowFileB)).thenReturn(outputB);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(outputA), anyMap())).thenReturn(writerA);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_B), same(outputB), anyMap())).thenReturn(writerB);
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);

        testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of());
        testGrouping.addRecord(testSession, byteRecord(1), RECORD_B, SCHEMA_B, Map.of(), Map.of());
        testGrouping.abortAllGroups();
        testGrouping.abortAllGroups();

        verify(writerA, times(1)).close();
        verify(writerB, times(1)).close();
    }

    @Test
    void testBeginFailureClosesWriterAndOutput() throws Exception {
        assertBeginFailureClosesWriterAndOutput(new IOException("begin failed"));
        assertBeginFailureClosesWriterAndOutput(new IllegalStateException("begin failed"));
        assertBeginFailureClosesWriterAndOutput(new AssertionError("begin failed"));
    }

    @Test
    void testBeginErrorPreservesCleanupErrors() throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writer = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFile = new MockFlowFile(1);
        final OutputStream output = mock(OutputStream.class);
        final AssertionError beginFailure = new AssertionError("begin failed");
        final AssertionError writerCloseFailure = new AssertionError("writer close failed");
        final AssertionError outputCloseFailure = new AssertionError("output close failed");
        when(testSession.create()).thenReturn(flowFile);
        when(testSession.putAllAttributes(same(flowFile), anyMap())).thenReturn(flowFile);
        when(testSession.write(flowFile)).thenReturn(output);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(output), anyMap())).thenReturn(writer);
        doThrow(beginFailure).when(writer).beginRecordSet();
        doThrow(writerCloseFailure).when(writer).close();
        doThrow(outputCloseFailure).when(output).close();
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);

        final AssertionError thrown = assertThrows(AssertionError.class,
                () -> testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of()));

        assertEquals(List.of(writerCloseFailure, outputCloseFailure), List.of(thrown.getSuppressed()));
        verify(testSession).remove(flowFile);
        testGrouping.abortAllGroups();
    }

    @Test
    void testInitializationCleanupHandlesRepeatedErrorInstance() throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writer = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFile = new MockFlowFile(1);
        final OutputStream output = mock(OutputStream.class);
        final AssertionError failure = new AssertionError("repeated failure");
        when(testSession.create()).thenReturn(flowFile);
        when(testSession.putAllAttributes(same(flowFile), anyMap())).thenReturn(flowFile);
        when(testSession.write(flowFile)).thenReturn(output);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(output), anyMap())).thenReturn(writer);
        doThrow(failure).when(writer).beginRecordSet();
        doThrow(failure).when(writer).close();
        doThrow(failure).when(output).close();
        doThrow(failure).when(testSession).remove(flowFile);
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);

        final AssertionError thrown = assertThrows(AssertionError.class,
                () -> testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of()));

        assertEquals(failure, thrown);
        assertEquals(0, thrown.getSuppressed().length);
        verify(writer).close();
        verify(output).close();
        verify(testSession).remove(flowFile);
        testGrouping.abortAllGroups();
    }

    @Test
    void testAbortContinuesAfterWriterError() throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writerA = mock(RecordSetWriter.class);
        final RecordSetWriter writerB = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFileA = new MockFlowFile(1);
        final FlowFile flowFileB = new MockFlowFile(2);
        final OutputStream outputA = mock(OutputStream.class);
        final OutputStream outputB = mock(OutputStream.class);
        final AssertionError closeFailure = new AssertionError("close failed");
        when(testSession.create()).thenReturn(flowFileA, flowFileB);
        when(testSession.putAllAttributes(same(flowFileA), anyMap())).thenReturn(flowFileA);
        when(testSession.putAllAttributes(same(flowFileB), anyMap())).thenReturn(flowFileB);
        when(testSession.write(flowFileA)).thenReturn(outputA);
        when(testSession.write(flowFileB)).thenReturn(outputB);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(outputA), anyMap())).thenReturn(writerA);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_B), same(outputB), anyMap())).thenReturn(writerB);
        doThrow(closeFailure).when(writerA).close();
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);
        testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of());
        testGrouping.addRecord(testSession, byteRecord(1), RECORD_B, SCHEMA_B, Map.of(), Map.of());

        assertEquals(closeFailure, assertThrows(AssertionError.class, testGrouping::abortAllGroups));

        verify(writerA).close();
        verify(writerB).close();
        testGrouping.abortAllGroups();
    }

    @Test
    void testAbortHandlesRepeatedWriterErrorInstance() throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writerA = mock(RecordSetWriter.class);
        final RecordSetWriter writerB = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFileA = new MockFlowFile(1);
        final FlowFile flowFileB = new MockFlowFile(2);
        final OutputStream outputA = mock(OutputStream.class);
        final OutputStream outputB = mock(OutputStream.class);
        final AssertionError failure = new AssertionError("repeated close failure");
        when(testSession.create()).thenReturn(flowFileA, flowFileB);
        when(testSession.putAllAttributes(same(flowFileA), anyMap())).thenReturn(flowFileA);
        when(testSession.putAllAttributes(same(flowFileB), anyMap())).thenReturn(flowFileB);
        when(testSession.write(flowFileA)).thenReturn(outputA);
        when(testSession.write(flowFileB)).thenReturn(outputB);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(outputA), anyMap())).thenReturn(writerA);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_B), same(outputB), anyMap())).thenReturn(writerB);
        doThrow(failure).when(writerA).close();
        doThrow(failure).when(writerB).close();
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);
        testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of());
        testGrouping.addRecord(testSession, byteRecord(1), RECORD_B, SCHEMA_B, Map.of(), Map.of());

        assertEquals(failure, assertThrows(AssertionError.class, testGrouping::abortAllGroups));
        assertEquals(0, failure.getSuppressed().length);
        verify(writerA).close();
        verify(writerB).close();
        testGrouping.abortAllGroups();
    }

    private void assertBeginFailureClosesWriterAndOutput(final Throwable failure) throws Exception {
        final RecordSetWriterFactory testWriterFactory = mock(RecordSetWriterFactory.class);
        final RecordSetWriter writer = mock(RecordSetWriter.class);
        final ProcessSession testSession = mock(ProcessSession.class);
        final FlowFile flowFile = new MockFlowFile(1);
        final OutputStream output = mock(OutputStream.class);
        when(testSession.create()).thenReturn(flowFile);
        when(testSession.putAllAttributes(same(flowFile), anyMap())).thenReturn(flowFile);
        when(testSession.write(flowFile)).thenReturn(output);
        when(testWriterFactory.createWriter(same(logger), same(SCHEMA_A), same(output), anyMap())).thenReturn(writer);
        doThrow(failure).when(writer).beginRecordSet();
        final CreateNewFlowFileGrouping testGrouping = new CreateNewFlowFileGrouping(testWriterFactory, logger, BROKER_URI, true);

        assertThrows(failure.getClass(),
                () -> testGrouping.addRecord(testSession, byteRecord(0), RECORD_A, SCHEMA_A, Map.of(), Map.of()));

        verify(writer).close();
        verify(output).close();
        verify(testSession).remove(flowFile);
        testGrouping.abortAllGroups();
    }

    private static ByteRecord byteRecord(final long offset) {
        return new ByteRecord(TOPIC, 0, offset, offset, List.of(), null, new byte[0], 0L);
    }

    private static final class PassThroughSchemaRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
        private final MockRecordWriter writer = new MockRecordWriter(null, false);

        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out,
                final Map<String, String> variables) throws IOException {
            return writer.createWriter(logger, schema, out, variables);
        }
    }
}
