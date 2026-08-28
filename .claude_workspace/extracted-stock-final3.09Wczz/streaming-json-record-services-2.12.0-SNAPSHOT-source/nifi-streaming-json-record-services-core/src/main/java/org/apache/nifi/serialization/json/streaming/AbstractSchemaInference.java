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

import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class AbstractSchemaInference<T> implements SchemaInferenceEngine<T> {
    protected RecordSchema createSchema(final Map<String, FieldTypeInference> inferences, final String rootElementName) {
        final List<RecordField> recordFields = new ArrayList<>(inferences.size());
        inferences.forEach((fieldName, type) -> recordFields.add(new RecordField(fieldName, type.toDataType())));
        final SimpleRecordSchema schema = new SimpleRecordSchema(recordFields);
        schema.setSchemaName(rootElementName);
        return schema;
    }

    protected RecordSchema defaultArrayTypes(final RecordSchema recordSchema) {
        return defaultArrayTypes(recordSchema, false);
    }

    protected RecordSchema defaultArrayTypes(final RecordSchema recordSchema, final boolean rebuildContainers) {
        final List<RecordField> recordFields = recordSchema.getFields();
        List<RecordField> adjustedFields = rebuildContainers ? new ArrayList<>(recordFields) : null;
        for (int i = 0; i < recordFields.size(); i++) {
            final RecordField recordField = recordFields.get(i);
            final RecordField adjustedField = defaultArrayTypes(recordField, rebuildContainers);
            if (adjustedField != recordField) {
                if (adjustedFields == null) {
                    adjustedFields = new ArrayList<>(recordFields);
                }
                adjustedFields.set(i, adjustedField);
            }
        }
        return adjustedFields == null ? recordSchema : new SimpleRecordSchema(adjustedFields, recordSchema.getIdentifier());
    }

    private RecordField defaultArrayTypes(final RecordField recordField, final boolean rebuildContainers) {
        final DataType dataType = recordField.getDataType();
        final DataType adjustedDataType = defaultArrayType(dataType, rebuildContainers);
        if (adjustedDataType == dataType) {
            return recordField;
        }
        return new RecordField(recordField.getFieldName(), adjustedDataType, recordField.getDefaultValue(), recordField.getAliases(), recordField.isNullable());
    }

    private DataType defaultArrayType(final DataType dataType, final boolean rebuildContainers) {
        return switch (dataType.getFieldType()) {
            case ARRAY -> {
                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                final DataType elementType = arrayDataType.getElementType();
                final DataType adjustedElementType = elementType == null
                        ? RecordFieldType.STRING.getDataType() : defaultArrayType(elementType, rebuildContainers);
                yield !rebuildContainers && adjustedElementType == elementType
                        ? dataType : RecordFieldType.ARRAY.getArrayDataType(adjustedElementType);
            }
            case RECORD -> {
                final RecordDataType recordDataType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                final RecordSchema adjustedChildSchema = defaultArrayTypes(childSchema, rebuildContainers);
                yield adjustedChildSchema == childSchema
                        ? dataType : RecordFieldType.RECORD.getRecordDataType(adjustedChildSchema);
            }
            case CHOICE -> {
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final Set<DataType> adjustedTypes = new LinkedHashSet<>(choiceDataType.getPossibleSubTypes().size());
                for (final DataType possibleType : choiceDataType.getPossibleSubTypes()) {
                    adjustedTypes.add(defaultArrayType(possibleType, rebuildContainers));
                }
                if (adjustedTypes.size() == 1) {
                    yield adjustedTypes.iterator().next();
                }
                final List<DataType> adjustedTypeList = new ArrayList<>(adjustedTypes);
                yield !rebuildContainers && adjustedTypeList.equals(choiceDataType.getPossibleSubTypes())
                        ? dataType : RecordFieldType.CHOICE.getChoiceDataType(adjustedTypeList);
            }
            default -> dataType;
        };
    }
}
