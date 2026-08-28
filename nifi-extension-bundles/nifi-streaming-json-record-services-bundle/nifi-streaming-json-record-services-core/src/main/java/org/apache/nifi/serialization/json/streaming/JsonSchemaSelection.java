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

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Queue;
import java.util.Set;

final class JsonSchemaSelection {
    private JsonSchemaSelection() {
    }

    static RecordSchema select(final RecordSchema rootSchema, final String fieldName) {
        if (rootSchema == null) {
            throw new IllegalArgumentException("Root schema is required for nested field selection");
        }
        if (fieldName == null) {
            throw new IllegalArgumentException("Selected schema field name is required");
        }

        final Queue<RecordSchema> schemas = new ArrayDeque<>();
        final Set<RecordSchema> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        schemas.add(rootSchema);
        visited.add(rootSchema);
        while (!schemas.isEmpty()) {
            final RecordSchema currentSchema = schemas.remove();
            final RecordField selectedField = currentSchema.getField(fieldName).orElse(null);
            if (selectedField != null) {
                return requireChildSchema(selectedField);
            }
            for (final RecordField field : currentSchema.getFields()) {
                final RecordSchema childSchema = getChildSchema(field);
                if (childSchema != null && visited.add(childSchema)) {
                    schemas.add(childSchema);
                }
            }
        }
        throw new IllegalArgumentException("Selected schema field [%s] not found".formatted(fieldName));
    }

    private static RecordSchema requireChildSchema(final RecordField field) {
        final RecordSchema childSchema = getChildSchema(field);
        if (childSchema == null) {
            throw new IllegalArgumentException("Selected schema field [%s] is not record or array-of-record type".formatted(field.getFieldName()));
        }
        return childSchema;
    }

    private static RecordSchema getChildSchema(final RecordField field) {
        if (field.getDataType() instanceof final RecordDataType recordType) {
            return recordType.getChildSchema();
        }
        if (field.getDataType() instanceof final ArrayDataType arrayType
                && arrayType.getElementType() instanceof final RecordDataType recordType) {
            return recordType.getChildSchema();
        }
        return null;
    }
}
