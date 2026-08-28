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
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.List;

final class SchemaMutationSnapshot {
    private static final int INITIAL_STATE_CAPACITY = 8;
    private static final SchemaMutationSnapshot UNSUPPORTED = new SchemaMutationSnapshot(null, false);

    private Object[] schemaStates;
    private int schemaCount;
    private final boolean supported;

    private SchemaMutationSnapshot(final Object[] schemaStates, final boolean supported) {
        this.schemaStates = schemaStates;
        this.supported = supported;
    }

    static SchemaMutationSnapshot capture(final RecordSchema schema) {
        final SchemaMutationSnapshot snapshot = new SchemaMutationSnapshot(new Object[INITIAL_STATE_CAPACITY], true);
        return snapshot.captureSchema(schema) ? snapshot : UNSUPPORTED;
    }

    private boolean captureSchema(final RecordSchema schema) {
        if (schema == null || containsSchema(schema)) {
            return true;
        }
        if (schema.getClass() != SimpleRecordSchema.class) {
            return false;
        }

        final List<RecordField> fields = schema.getFields();
        addSchema(schema, fields);
        for (final RecordField field : fields) {
            if (!captureType(field.getDataType())) {
                return false;
            }
        }
        return true;
    }

    private boolean captureType(final DataType dataType) {
        if (dataType == null) {
            return true;
        }
        return switch (dataType.getFieldType()) {
            case RECORD -> captureSchema(((RecordDataType) dataType).getChildSchema());
            case ARRAY -> captureType(((ArrayDataType) dataType).getElementType());
            case MAP -> captureType(((MapDataType) dataType).getValueType());
            case CHOICE -> {
                boolean captured = true;
                for (final DataType possibleType : ((ChoiceDataType) dataType).getPossibleSubTypes()) {
                    captured &= captureType(possibleType);
                }
                yield captured;
            }
            default -> true;
        };
    }

    private boolean containsSchema(final RecordSchema schema) {
        for (int i = 0; i < schemaCount; i++) {
            if (schemaStates[i << 1] == schema) {
                return true;
            }
        }
        return false;
    }

    private void addSchema(final RecordSchema schema, final List<RecordField> fields) {
        final int offset = schemaCount << 1;
        if (offset == schemaStates.length) {
            final Object[] expanded = new Object[schemaStates.length << 1];
            System.arraycopy(schemaStates, 0, expanded, 0, schemaStates.length);
            schemaStates = expanded;
        }
        schemaStates[offset] = schema;
        schemaStates[offset + 1] = fields;
        schemaCount++;
    }

    boolean isUnmodified() {
        if (!supported) {
            return false;
        }
        for (int i = 0; i < schemaCount; i++) {
            final int offset = i << 1;
            if (((RecordSchema) schemaStates[offset]).getFields() != schemaStates[offset + 1]) {
                return false;
            }
        }
        return true;
    }
}
