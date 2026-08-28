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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStreamingJsonRecordSetWriter {

    @Test
    void testFormattedStringRequiresTimestampFormat() throws InitializationException {
        final StreamingJsonRecordSetWriter service = new StreamingJsonRecordSetWriter();
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("writer", service);
        runner.setProperty(service, StreamingJsonRecordSetWriter.TIMESTAMP_REPRESENTATION, TimestampRepresentation.FORMATTED_STRING.name());

        assertThrows(IllegalStateException.class, () -> runner.enableControllerService(service));

        runner.setProperty(service, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSX");
        runner.enableControllerService(service);
    }

    @Test
    void testMigrateProperties() {
        final StreamingJsonRecordSetWriter service = new StreamingJsonRecordSetWriter();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("suppress-nulls", StreamingJsonRecordSetWriter.SUPPRESS_NULLS.getName()),
                Map.entry("output-grouping", StreamingJsonRecordSetWriter.OUTPUT_GROUPING.getName()),
                Map.entry("compression-format", StreamingJsonRecordSetWriter.COMPRESSION_FORMAT.getName()),
                Map.entry("compression-level", StreamingJsonRecordSetWriter.COMPRESSION_LEVEL.getName()),
                Map.entry("schema-cache", SchemaRegistryRecordSetWriter.SCHEMA_CACHE.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_ACCESS_STRATEGY_PROPERTY_NAME, SCHEMA_ACCESS_STRATEGY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REGISTRY_PROPERTY_NAME, SCHEMA_REGISTRY.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_NAME_PROPERTY_NAME, SCHEMA_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_BRANCH_NAME_PROPERTY_NAME, SCHEMA_BRANCH_NAME.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_VERSION_PROPERTY_NAME, SCHEMA_VERSION.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_TEXT_PROPERTY_NAME, SCHEMA_TEXT.getName()),
                Map.entry(SchemaAccessUtils.OLD_SCHEMA_REFERENCE_READER_PROPERTY_NAME, SCHEMA_REFERENCE_READER.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
        assertEquals(Set.of(StreamingJsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION.getName()), result.getPropertiesUpdated());
        assertEquals("true", configuration.getRawPropertyValue(
                StreamingJsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION.getName()).orElseThrow());

        final Set<String> expectedRemoved = Set.of("schema-protocol-version");
        assertEquals(expectedRemoved, result.getPropertiesRemoved());
    }

    @Test
    void testMigratePropertiesPreservesConfiguredScientificNotation() {
        final StreamingJsonRecordSetWriter service = new StreamingJsonRecordSetWriter();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(Map.of(
                StreamingJsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION.getName(), "false"));

        service.migrateProperties(configuration);

        assertEquals("false", configuration.getRawPropertyValue(
                StreamingJsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION.getName()).orElseThrow());
        assertFalse(configuration.toPropertyMigrationResult().getPropertiesUpdated().contains(
                StreamingJsonRecordSetWriter.ALLOW_SCIENTIFIC_NOTATION.getName()));
    }

    @Test
    void testGzipWriterReusesValidatedJsonWithoutMaterialization() throws Exception {
        final StreamingJsonRecordSetWriter service = new StreamingJsonRecordSetWriter();
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("writer", service);
        runner.setProperty(service, StreamingJsonRecordSetWriter.COMPRESSION_FORMAT,
                StreamingJsonRecordSetWriter.COMPRESSION_FORMAT_GZIP);
        runner.enableControllerService(service);

        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.INT.getDataType())));
        final byte[] json = "{\"id\":42}".getBytes(StandardCharsets.UTF_8);
        final DeferredJsonRecord record = new DeferredJsonRecord(schema, true, false,
                SerializedForm.of(new Utf8JsonValue(json), "application/json"),
                () -> {
                    throw new AssertionError("Validated JSON should remain unmaterialized");
                });

        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (RecordSetWriter writer = service.createWriter(runner.getLogger(), schema, compressed, Map.of())) {
            writer.write(RecordSet.of(schema, record));
        }

        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed.toByteArray()))) {
            assertEquals("[{\"id\":42}]", new String(gzip.readAllBytes(), StandardCharsets.UTF_8));
        }
        assertFalse(record.isMaterialized());
    }
}
