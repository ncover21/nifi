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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class TestJsonSchemaSelection {
    @Test
    void testSelectsRecordArrayBeyondRecursiveReference() {
        final RecordSchema selectedSchema = schema(new RecordField("id", RecordFieldType.INT.getDataType()));
        final RecordSchema wrapperSchema = schema(new RecordField("events",
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(selectedSchema))));
        final SimpleRecordSchema rootSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        rootSchema.setFields(List.of(
                new RecordField("parent", RecordFieldType.RECORD.getRecordDataType(rootSchema)),
                new RecordField("wrapper", RecordFieldType.RECORD.getRecordDataType(wrapperSchema))));

        assertSame(selectedSchema, JsonSchemaSelection.select(rootSchema, "events"));
    }

    @Test
    void testMissingFieldInRecursiveSchemaRejected() {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        schema.setFields(List.of(new RecordField("child", RecordFieldType.RECORD.getRecordDataType(schema))));

        final IllegalArgumentException exception = assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertThrows(IllegalArgumentException.class, () -> JsonSchemaSelection.select(schema, "events")));

        assertEquals("Selected schema field [events] not found", exception.getMessage());
    }

    @Test
    void testStructurallyEqualRecursiveSchemasUseIdentityForCycleDetection() {
        final SimpleRecordSchema first = recursiveSchema();
        final SimpleRecordSchema second = recursiveSchema();
        final RecordSchema rootSchema = schema(
                new RecordField("first", RecordFieldType.RECORD.getRecordDataType(first)),
                new RecordField("second", RecordFieldType.RECORD.getRecordDataType(second)));

        final IllegalArgumentException exception = assertTimeoutPreemptively(Duration.ofSeconds(1),
                () -> assertThrows(IllegalArgumentException.class, () -> JsonSchemaSelection.select(rootSchema, "missing")));

        assertEquals("Selected schema field [missing] not found", exception.getMessage());
    }

    @Test
    void testScalarSelectedFieldRejected() {
        final RecordSchema schema = schema(new RecordField("events", RecordFieldType.STRING.getDataType()));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> JsonSchemaSelection.select(schema, "events"));

        assertEquals("Selected schema field [events] is not record or array-of-record type", exception.getMessage());
    }

    @Test
    void testScalarArraySelectedFieldRejected() {
        final RecordSchema schema = schema(new RecordField("events",
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> JsonSchemaSelection.select(schema, "events"));

        assertEquals("Selected schema field [events] is not record or array-of-record type", exception.getMessage());
    }

    @Test
    void testMissingFieldNameRejected() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> JsonSchemaSelection.select(schema(), null));

        assertEquals("Selected schema field name is required", exception.getMessage());
    }

    @Test
    void testMissingRootSchemaRejected() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> JsonSchemaSelection.select(null, "events"));

        assertEquals("Root schema is required for nested field selection", exception.getMessage());
    }

    private static SimpleRecordSchema recursiveSchema() {
        final SimpleRecordSchema schema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
        schema.setFields(List.of(new RecordField("child", RecordFieldType.RECORD.getRecordDataType(schema))));
        return schema;
    }

    private static RecordSchema schema(final RecordField... fields) {
        return new SimpleRecordSchema(List.of(fields));
    }
}
