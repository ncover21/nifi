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
package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.lookup.ReaderLookup;
import org.apache.nifi.lookup.RecordSetWriterLookup;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader;
import org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStreamingJsonRecordServicesCompatibility {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String EVENT_SCHEMA = """
            {"type":"record","name":"Event","fields":[
              {"name":"id","type":"int"},
              {"name":"name","type":["null","string"],"default":null}
            ]}
            """;

    @ParameterizedTest(name = "streaming reader={0}, streaming writer={1}")
    @MethodSource("servicePairings")
    void testConvertRecordPairings(final boolean streamingReader, final boolean streamingWriter) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        configureServices(runner, streamingReader, streamingWriter);
        final String input = """
                { "id" : 1, "name" : "Ada" }
                { "id" : 2, "name" : "Lin" }
                """;

        runner.enqueue(input);
        runner.run();

        runner.assertTransferCount(ConvertRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertRecord.REL_FAILURE, 0);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        output.assertAttributeEquals("record.count", "2");
        output.assertAttributeEquals("mime.type", "application/json");
        assertEquals(OBJECT_MAPPER.readTree("[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Lin\"}]"), readJson(output));
        if (streamingReader && streamingWriter) {
            final String content = output.getContent();
            assertTrue(content.contains("{ \"id\" : 1, \"name\" : \"Ada\" }"));
            assertTrue(content.contains("{ \"id\" : 2, \"name\" : \"Lin\" }"));
        }
    }

    @Test
    void testQueryRecordMaterializesAndProjects() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(QueryRecord.class);
        runner.setValidateExpressionUsage(false);
        configureServices(runner, true, true);
        runner.setProperty("adults", "SELECT name FROM FLOWFILE WHERE age >= 18");
        runner.enqueue("[{\"name\":\"Ada\",\"age\":37,\"ignored\":\"x\"},{\"name\":\"Tim\",\"age\":16,\"ignored\":\"y\"}]");

        runner.run();

        runner.assertTransferCount("adults", 1);
        runner.assertTransferCount(QueryRecord.REL_FAILURE, 0);
        final MockFlowFile output = runner.getFlowFilesForRelationship("adults").getFirst();
        output.assertAttributeEquals(QueryRecord.ROUTE_ATTRIBUTE_KEY, "adults");
        output.assertAttributeEquals("record.count", "1");
        assertEquals(OBJECT_MAPPER.readTree("[{\"name\":\"Ada\"}]"), readJson(output));
    }

    @ParameterizedTest
    @CsvSource({
            "/status,direct-new",
            "/profile/name,nested-new"
    })
    void testUpdateRecordInvalidatesSerializedInput(final String recordPath, final String replacement) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(UpdateRecord.class);
        configureServices(runner, true, true);
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);
        runner.setProperty(recordPath, replacement);
        runner.enqueue("[{\"status\":\"direct-old\",\"profile\":{\"name\":\"nested-old\"},\"untouched\":\"kept\"}]");

        runner.run();

        runner.assertTransferCount(UpdateRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(UpdateRecord.REL_FAILURE, 0);
        final JsonNode output = readJson(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).getFirst());
        assertEquals(replacement, output.at("/0" + recordPath).textValue());
        assertEquals("kept", output.at("/0/untouched").textValue());
    }

    @Test
    void testPartitionRecordRetainsInterleavedRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(PartitionRecord.class);
        configureServices(runner, true, true);
        runner.setProperty("tenant", "/tenant");
        runner.enqueue("[{\"tenant\":\"A\",\"sequence\":1},{\"tenant\":\"B\",\"sequence\":2},{\"tenant\":\"A\",\"sequence\":3}]");

        runner.run();

        runner.assertTransferCount(PartitionRecord.REL_SUCCESS, 2);
        runner.assertTransferCount(PartitionRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(PartitionRecord.REL_FAILURE, 0);
        final Map<String, List<Integer>> sequences = new java.util.HashMap<>();
        for (final MockFlowFile flowFile : runner.getFlowFilesForRelationship(PartitionRecord.REL_SUCCESS)) {
            final List<Integer> values = new ArrayList<>();
            readJson(flowFile).forEach(record -> values.add(record.get("sequence").intValue()));
            sequences.put(flowFile.getAttribute("tenant"), values);
        }
        assertEquals(List.of(1, 3), sequences.get("A"));
        assertEquals(List.of(2), sequences.get("B"));
    }

    @Test
    void testSplitRecordPreservesRootArrayBoundaries() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(SplitRecord.class);
        configureServices(runner, true, true);
        runner.setProperty(SplitRecord.RECORDS_PER_SPLIT, "2");
        runner.enqueue("[{\"id\":1},{\"id\":2},{\"id\":3},{\"id\":4},{\"id\":5}]");

        runner.run();

        runner.assertTransferCount(SplitRecord.REL_SPLITS, 3);
        runner.assertTransferCount(SplitRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(SplitRecord.REL_FAILURE, 0);
        final List<Integer> identifiers = new ArrayList<>();
        for (final MockFlowFile split : runner.getFlowFilesForRelationship(SplitRecord.REL_SPLITS)) {
            readJson(split).forEach(record -> identifiers.add(record.get("id").intValue()));
        }
        identifiers.sort(Integer::compareTo);
        assertEquals(List.of(1, 2, 3, 4, 5), identifiers);
    }

    @Test
    void testMergeRecordRetainsRecordsAcrossFlowFiles() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(MergeRecord.class);
        configureServices(runner, true, true);
        runner.setProperty(MergeRecord.MIN_RECORDS, "4");
        runner.setProperty(MergeRecord.MAX_RECORDS, "4");
        runner.enqueue("[{\"id\":1},{\"id\":2}]");
        runner.enqueue("[{\"id\":3},{\"id\":4}]");

        runner.run(2);

        runner.assertTransferCount(MergeRecord.REL_MERGED, 1);
        runner.assertTransferCount(MergeRecord.REL_ORIGINAL, 2);
        runner.assertTransferCount(MergeRecord.REL_FAILURE, 0);
        final JsonNode output = readJson(runner.getFlowFilesForRelationship(MergeRecord.REL_MERGED).getFirst());
        assertEquals(List.of(1, 2, 3, 4), java.util.stream.StreamSupport.stream(output.spliterator(), false)
                .map(record -> record.get("id").intValue())
                .sorted()
                .toList());
    }

    @Test
    void testStreamingReaderMaterializesForNonJsonWriter() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
        final CSVRecordSetWriter writer = new CSVRecordSetWriter();
        addReader(runner, "reader", reader);
        addWriter(runner, "writer", writer);
        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");
        runner.enqueue("[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Lin\"}]");

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        output.assertAttributeEquals("record.count", "2");
        output.assertAttributeEquals("mime.type", "text/csv");
        output.assertContentEquals("id,name\n1,Ada\n2,Lin\n");
    }

    @Test
    void testNonJsonReaderWritesStreamingJson() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        final CSVReader reader = new CSVReader();
        final StreamingJsonRecordSetWriter writer = new StreamingJsonRecordSetWriter();
        addReader(runner, "reader", reader);
        addWriter(runner, "writer", writer);
        runner.setProperty(ConvertRecord.RECORD_READER, "reader");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");
        runner.enqueue("id,name\n1,Ada\n2,Lin\n");

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        output.assertAttributeEquals("record.count", "2");
        output.assertAttributeEquals("mime.type", "application/json");
        assertEquals(OBJECT_MAPPER.readTree("[{\"id\":1,\"name\":\"Ada\"},{\"id\":2,\"name\":\"Lin\"}]"), readJson(output));
    }

    @Test
    void testMalformedTailRoutesOriginalToFailure() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        final Services services = configureServices(runner, true, true);
        final ControllerService reader = (ControllerService) services.reader();
        runner.disableControllerService(reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY,
                SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, EVENT_SCHEMA);
        runner.enableControllerService(reader);
        final String input = "{\"id\":1,\"name\":\"ok\"}\n{\"id\":";
        runner.enqueue(input);

        runner.run();

        runner.assertTransferCount(ConvertRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(ConvertRecord.REL_FAILURE, 1);
        final MockFlowFile failure = runner.getFlowFilesForRelationship(ConvertRecord.REL_FAILURE).getFirst();
        failure.assertContentEquals(input);
        assertFalse(failure.getAttribute("record.error.message").isBlank());
    }

    @Test
    void testValidateRecordSplitsValidAndInvalidRecords() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ValidateRecord.class);
        configureServices(runner, true, true);
        runner.setProperty(ValidateRecord.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(SchemaAccessUtils.SCHEMA_TEXT, EVENT_SCHEMA);
        runner.setProperty(ValidateRecord.STRICT_TYPE_CHECKING, "true");
        runner.setProperty(ValidateRecord.VALIDATION_DETAILS_ATTRIBUTE_NAME, "validation.details");
        runner.enqueue("[{\"id\":1,\"name\":\"valid\"},{\"id\":\"not-an-integer\",\"name\":\"invalid\"}]");

        runner.run();

        runner.assertTransferCount(ValidateRecord.REL_VALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_INVALID, 1);
        runner.assertTransferCount(ValidateRecord.REL_FAILURE, 0);
        final MockFlowFile valid = runner.getFlowFilesForRelationship(ValidateRecord.REL_VALID).getFirst();
        valid.assertAttributeEquals("record.count", "1");
        assertEquals(OBJECT_MAPPER.readTree("[{\"id\":1,\"name\":\"valid\"}]"), readJson(valid));
        final MockFlowFile invalid = runner.getFlowFilesForRelationship(ValidateRecord.REL_INVALID).getFirst();
        invalid.assertAttributeEquals("record.count", "1");
        assertEquals(OBJECT_MAPPER.readTree("[{\"id\":\"not-an-integer\",\"name\":\"invalid\"}]"), readJson(invalid));
        assertTrue(invalid.getAttribute("validation.details").contains("id"));
    }

    @Test
    void testLookupServicesDelegateToStreamingPair() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
        final StreamingJsonRecordSetWriter writer = new StreamingJsonRecordSetWriter();
        addReader(runner, "streaming-reader", reader);
        addWriter(runner, "streaming-writer", writer);

        final ReaderLookup readerLookup = new ReaderLookup();
        runner.addControllerService("reader-lookup", readerLookup);
        runner.setProperty(readerLookup, "streaming", "streaming-reader");
        runner.enableControllerService(readerLookup);
        final RecordSetWriterLookup writerLookup = new RecordSetWriterLookup();
        runner.addControllerService("writer-lookup", writerLookup);
        runner.setProperty(writerLookup, "streaming", "streaming-writer");
        runner.enableControllerService(writerLookup);
        runner.setProperty(ConvertRecord.RECORD_READER, "reader-lookup");
        runner.setProperty(ConvertRecord.RECORD_WRITER, "writer-lookup");
        final String input = "{ \"id\" : 1, \"name\" : \"Ada\" }";
        runner.enqueue(input, Map.of("recordreader.name", "streaming", "recordsetwriter.name", "streaming"));

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);
        final MockFlowFile output = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).getFirst();
        assertEquals(OBJECT_MAPPER.readTree("[{\"id\":1,\"name\":\"Ada\"}]"), readJson(output));
        assertTrue(output.getContent().contains(input));
    }

    private Services configureServices(final TestRunner runner, final boolean streamingReader,
                                       final boolean streamingWriter) throws Exception {
        final RecordReaderFactory reader = streamingReader ? new StreamingJsonRecordReader() : new JsonTreeReader();
        final RecordSetWriterFactory writer = streamingWriter ? new StreamingJsonRecordSetWriter() : new JsonRecordSetWriter();
        addReader(runner, "reader", reader);
        addWriter(runner, "writer", writer);
        if (runner.getProcessor() instanceof QueryRecord) {
            runner.setProperty(QueryRecord.RECORD_READER_FACTORY, "reader");
            runner.setProperty(QueryRecord.RECORD_WRITER_FACTORY, "writer");
        } else if (runner.getProcessor() instanceof UpdateRecord) {
            runner.setProperty(UpdateRecord.RECORD_READER, "reader");
            runner.setProperty(UpdateRecord.RECORD_WRITER, "writer");
        } else if (runner.getProcessor() instanceof PartitionRecord) {
            runner.setProperty(PartitionRecord.RECORD_READER, "reader");
            runner.setProperty(PartitionRecord.RECORD_WRITER, "writer");
        } else if (runner.getProcessor() instanceof SplitRecord) {
            runner.setProperty(SplitRecord.RECORD_READER, "reader");
            runner.setProperty(SplitRecord.RECORD_WRITER, "writer");
        } else if (runner.getProcessor() instanceof MergeRecord) {
            runner.setProperty(MergeRecord.RECORD_READER, "reader");
            runner.setProperty(MergeRecord.RECORD_WRITER, "writer");
        } else if (runner.getProcessor() instanceof ValidateRecord) {
            runner.setProperty(ValidateRecord.RECORD_READER, "reader");
            runner.setProperty(ValidateRecord.RECORD_WRITER, "writer");
        } else {
            runner.setProperty(ConvertRecord.RECORD_READER, "reader");
            runner.setProperty(ConvertRecord.RECORD_WRITER, "writer");
        }
        return new Services(reader, writer);
    }

    private void addReader(final TestRunner runner, final String identifier, final RecordReaderFactory reader) throws Exception {
        final ControllerService service = (ControllerService) reader;
        runner.addControllerService(identifier, service);
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.enableControllerService(service);
    }

    private void addWriter(final TestRunner runner, final String identifier, final RecordSetWriterFactory writer) throws Exception {
        final ControllerService service = (ControllerService) writer;
        runner.addControllerService(identifier, service);
        runner.setProperty(service, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.enableControllerService(service);
    }

    private JsonNode readJson(final MockFlowFile flowFile) throws Exception {
        return OBJECT_MAPPER.readTree(flowFile.toByteArray());
    }

    private static Stream<Arguments> servicePairings() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(true, true)
        );
    }

    private record Services(RecordReaderFactory reader, RecordSetWriterFactory writer) {
    }
}
