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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestStreamingJsonControllerServices {
    private static final Set<Class<?>> SERVICE_TYPES = Set.of(
            StreamingJsonRecordReader.class,
            StreamingJsonRecordSetWriter.class
    );

    @Test
    void testControllerServiceProviderContainsExpectedServices() {
        final Set<Class<?>> provided = ServiceLoader.load(ControllerService.class).stream()
                .map(ServiceLoader.Provider::type)
                .filter(type -> type.getPackageName().equals(StreamingJsonRecordReader.class.getPackageName()))
                .collect(Collectors.toSet());

        assertEquals(SERVICE_TYPES, provided);
    }

    @Test
    void testServiceContractsAndDescriptors() {
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();
        final StreamingJsonRecordSetWriter writer = new StreamingJsonRecordSetWriter();

        assertInstanceOf(RecordReaderFactory.class, reader);
        assertInstanceOf(ByteArrayRecordReaderFactory.class, reader);
        assertInstanceOf(RecordSetWriterFactory.class, writer);
        assertFalse(reader.getPropertyDescriptors().isEmpty());
        assertFalse(writer.getPropertyDescriptors().isEmpty());
        assertNotNull(StreamingJsonRecordReader.class.getClassLoader().getResource(
                "docs/org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader/additionalDetails.md"));
    }

    @Test
    void testLenientByteFallbackIsRetainable() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader service = enableReader(runner, ParsingStrategy.LENIENT);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), "{'number':42}".getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            assertRetainable(reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testDeferredDirectByteReaderHasRetainableGuarantee() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader service = enableReader(runner, ParsingStrategy.STANDARD);

        try (RecordReader reader = service.createRecordReaderFromBytes(
                Map.of(), "{\"number\":42}".getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            assertRetainable(reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testNonUtf8ByteFallbackIsRetainable() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader service = enableReader(runner, ParsingStrategy.STANDARD);
        final byte[] input = "{\"number\":42}".getBytes(StandardCharsets.UTF_16LE);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), input, runner.getLogger())) {
            assertRetainable(reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
        }
    }

    @Test
    void testContentReferenceByteFallbackIsRetainable() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("event", new SimpleRecordSchema(List.of(
                new RecordField("number", RecordFieldType.INT.getDataType()))));
        final PrefixSchemaReferenceReader referenceReader = new PrefixSchemaReferenceReader();
        final StreamingJsonRecordReader service = new StreamingJsonRecordReader();
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

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), "!{\"number\":42}".getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            assertRetainable(reader);
            assertEquals(42, reader.nextRecord().getValue("number"));
            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testChoiceSchemaFallbackIsRetainable() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final StreamingJsonRecordReader service = new StreamingJsonRecordReader();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(service, SCHEMA_TEXT, """
                {"type":"record","name":"event","fields":[{"name":"value","type":["int","string"]}]}
                """);
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
        runner.enableControllerService(service);

        try (RecordReader reader = service.createRecordReaderFromBytes(Map.of(), "{\"value\":42}".getBytes(StandardCharsets.UTF_8), runner.getLogger())) {
            assertRetainable(reader);
            assertEquals(42, reader.nextRecord().getValue("value"));
        }
    }

    private StreamingJsonRecordReader enableReader(final TestRunner runner, final ParsingStrategy parsingStrategy) throws Exception {
        final StreamingJsonRecordReader service = new StreamingJsonRecordReader();
        runner.addControllerService("reader", service);
        runner.setProperty(service, SCHEMA_ACCESS_STRATEGY, "infer-schema");
        runner.setProperty(service, AbstractJsonRowRecordReader.PARSING_STRATEGY, parsingStrategy.getValue());
        runner.enableControllerService(service);
        return service;
    }

    private void assertRetainable(final RecordReader reader) {
        assertEquals(RecordReader.RecordHandlingMode.RETAINABLE, reader.getRecordHandlingMode());
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
}
