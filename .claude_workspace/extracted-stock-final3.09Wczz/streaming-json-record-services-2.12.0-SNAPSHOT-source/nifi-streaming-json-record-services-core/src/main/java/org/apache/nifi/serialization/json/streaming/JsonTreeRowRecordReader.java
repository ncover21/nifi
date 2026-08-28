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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

final class JsonTreeRowRecordReader extends AbstractJsonRowRecordReader {

    private final RecordSchema schema;
    private boolean serializedFormModified;

    JsonTreeRowRecordReader(
            final InputStream in,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final SchemaApplicationStrategy schemaApplicationStrategy,
            final BiPredicate<String, String> captureFieldPredicate,
            final TokenParserFactory tokenParserFactory
    ) throws IOException, MalformedRecordException {

        this(in, logger, selectSchema(schema, startingFieldStrategy, startingFieldName, schemaApplicationStrategy),
                dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName, captureFieldPredicate, tokenParserFactory);
    }

    private JsonTreeRowRecordReader(
            final InputStream in,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final BiPredicate<String, String> captureFieldPredicate,
            final TokenParserFactory tokenParserFactory
    ) throws IOException, MalformedRecordException {

        super(in, logger, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName, captureFieldPredicate, tokenParserFactory);

        this.schema = schema;
    }

    private static RecordSchema selectSchema(final RecordSchema schema, final StartingFieldStrategy startingFieldStrategy,
                                             final String startingFieldName, final SchemaApplicationStrategy schemaApplicationStrategy) {
        return startingFieldStrategy == StartingFieldStrategy.NESTED_FIELD && schemaApplicationStrategy == SchemaApplicationStrategy.WHOLE_JSON
                ? JsonSchemaSelection.select(schema, startingFieldName) : schema;
    }

    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields)
            throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        serializedFormModified = false;
        return convertJsonNodeToRecord(jsonNode, schema, null, coerceTypes, dropUnknownFields, getSerializedForm(jsonNode));
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix, final boolean coerceTypes, final boolean dropUnknown)
            throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        return convertJsonNodeToRecord(jsonNode, schema, fieldNamePrefix, coerceTypes, dropUnknown, null);
    }

    private JsonNode getChildNode(final JsonNode jsonNode, final RecordField field) {
        if (jsonNode.has(field.getFieldName())) {
            return jsonNode.get(field.getFieldName());
        }

        for (final String alias : field.getAliases()) {
            if (jsonNode.has(alias)) {
                return jsonNode.get(alias);
            }
        }

        return null;
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix,
                                           final boolean coerceTypes, final boolean dropUnknown, final SerializedForm serializedForm)
            throws IOException, MalformedRecordException {

        final Map<String, Object> values = new LinkedHashMap<>(schema.getFieldCount() * 2);

        if (dropUnknown) {
            // Delete unknown fields for updated serialized representation
            final Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.properties().iterator();
            while (fields.hasNext()) {
                final Map.Entry<String, JsonNode> field = fields.next();
                final String fieldName = field.getKey();
                final Optional<RecordField> recordField = schema.getField(fieldName);
                if (recordField.isEmpty()) {
                    fields.remove();
                    serializedFormModified = true;
                }
            }

            for (final RecordField recordField : schema.getFields()) {
                final JsonNode childNode = getChildNode(jsonNode, recordField);
                if (childNode == null) {
                    continue;
                }

                final String fieldName = recordField.getFieldName();

                Object value;
                if (coerceTypes) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField.getDataType(), fieldName);
                }

                values.put(fieldName, value);
            }
        } else {
            final Iterator<String> fieldNames = jsonNode.fieldNames();
            while (fieldNames.hasNext()) {
                final String fieldName = fieldNames.next();
                final JsonNode childNode = jsonNode.get(fieldName);

                final RecordField recordField = schema.getField(fieldName).orElse(null);

                final Object value;
                if (coerceTypes && recordField != null) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType(), fieldName);
                }

                values.put(fieldName, value);
            }
        }

        final SerializedForm recordSerializedForm = serializedForm == null || serializedFormModified
                ? SerializedForm.of((Supplier<String>) jsonNode::toString, "application/json")
                : serializedForm;
        return new MapRecord(schema, values, recordSerializedForm, false, dropUnknown);
    }

    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType, final boolean dropUnknown) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case BIGINT:
            case LONG:
            case SHORT:
            case STRING:
            case ENUM:
            case DATE:
            case TIME:
            case UUID:
            case TIMESTAMP: {
                final Object rawValue = getRawNodeValue(fieldNode, fieldName);
                return DataTypeUtils.convertType(rawValue, desiredType, getDateFormat(), getTimeFormat(), getTimestampFormat(), fieldName);
            }
            case MAP: {
                final DataType valueType = ((MapDataType) desiredType).getValueType();

                final Map<String, Object> map = new LinkedHashMap<>();
                final Iterator<String> fieldNameItr = fieldNode.fieldNames();
                while (fieldNameItr.hasNext()) {
                    final String childName = fieldNameItr.next();
                    final JsonNode childNode = fieldNode.get(childName);
                    final Object childValue = convertField(childNode, fieldName, valueType, dropUnknown);
                    map.put(childName, childValue);
                }

                return map;
            }
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final DataType elementType = ((ArrayDataType) desiredType).getElementType();
                    final Object converted = convertField(node, fieldName, elementType, dropUnknown);
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    RecordSchema childSchema;
                    if (desiredType instanceof final RecordDataType recordDataType) {
                        childSchema = recordDataType.getChildSchema();
                    } else {
                        return null;
                    }

                    if (childSchema == null) {
                        final List<RecordField> fields = new ArrayList<>();
                        final Iterator<String> fieldNameItr = fieldNode.fieldNames();
                        while (fieldNameItr.hasNext()) {
                            fields.add(new RecordField(fieldNameItr.next(), RecordFieldType.STRING.getDataType()));
                        }

                        childSchema = new SimpleRecordSchema(fields);
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema, fieldName + ".", true, dropUnknown);
                } else {
                    return null;
                }
            }
            case CHOICE: {
                return DataTypeUtils.convertType(getRawNodeValue(fieldNode, desiredType, fieldName), desiredType, fieldName);
            }
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
