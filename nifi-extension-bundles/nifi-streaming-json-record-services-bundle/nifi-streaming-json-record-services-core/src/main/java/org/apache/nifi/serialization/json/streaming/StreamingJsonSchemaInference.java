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

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.NumberType;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.nifi.serialization.json.streaming.AbstractSchemaInference;
import org.apache.nifi.serialization.json.streaming.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class StreamingJsonSchemaInference extends AbstractSchemaInference<JsonParser> {
    private final TimeValueInference timeValueInference;
    private final int maximumSchemaFields;

    public StreamingJsonSchemaInference(final TimeValueInference timeValueInference) {
        this(timeValueInference, AbstractStreamingJsonRecordReaderService.DEFAULT_MAX_SCHEMA_INFERENCE_FIELDS);
    }

    StreamingJsonSchemaInference(final TimeValueInference timeValueInference, final int maximumSchemaFields) {
        if (maximumSchemaFields < 1) {
            throw new IllegalArgumentException("Maximum fields per object must be positive");
        }
        this.timeValueInference = timeValueInference;
        this.maximumSchemaFields = maximumSchemaFields;
    }

    @Override
    public RecordSchema inferSchema(final RecordSource<JsonParser> recordSource) throws IOException {
        return inferSchemaAndClose(recordSource, null).schema();
    }

    InferredJsonSchema inferSchemaWithMetadata(final RecordSource<JsonParser> recordSource) throws IOException {
        return inferSchemaAndClose(recordSource, new MetadataCollector(Integer.MAX_VALUE));
    }

    InferredJsonSchema inferSchemaWithMetadata(final RecordSource<JsonParser> recordSource, final int maximumRecords) throws IOException {
        return inferSchemaAndClose(recordSource, new MetadataCollector(maximumRecords));
    }

    private InferredJsonSchema inferSchemaAndClose(final RecordSource<JsonParser> recordSource, final MetadataCollector metadataCollector) throws IOException {
        Throwable inferenceFailure = null;
        try {
            return inferSchema(recordSource, metadataCollector);
        } catch (final IOException | RuntimeException | Error e) {
            inferenceFailure = e;
            throw e;
        } finally {
            closeRecordSource(recordSource, inferenceFailure);
        }
    }

    private void closeRecordSource(final RecordSource<JsonParser> recordSource, final Throwable inferenceFailure) throws IOException {
        if (!(recordSource instanceof final AutoCloseable closeable)) {
            return;
        }

        try {
            closeable.close();
        } catch (final IOException | RuntimeException | Error e) {
            if (inferenceFailure == null) {
                throw e;
            }
            if (inferenceFailure != e) {
                inferenceFailure.addSuppressed(e);
            }
        } catch (final Exception e) {
            final IOException closeFailure = new IOException("Failed to close schema inference record source", e);
            if (inferenceFailure == null) {
                throw closeFailure;
            }
            if (inferenceFailure != closeFailure) {
                inferenceFailure.addSuppressed(closeFailure);
            }
        }
    }

    private InferredJsonSchema inferSchema(final RecordSource<JsonParser> recordSource, final MetadataCollector metadataCollector) throws IOException {
        JsonParser parser = recordSource.next();
        if (parser == null) {
            return new InferredJsonSchema(new SimpleRecordSchema(List.of()), List.of(), true);
        }

        final RecordState firstRecordState = createRecordState(parser, metadataCollector);
        final Map<String, DataType> firstRecordTypes = readObjectTypes(parser, firstRecordState);
        addRecordMetadata(metadataCollector, parser, firstRecordState);
        parser = recordSource.next();
        if (parser == null) {
            return inferredSchema(defaultArrayTypes(createSchemaFromTypes(firstRecordTypes)), metadataCollector);
        }

        final Map<String, FieldTypeInference> inferences = new LinkedHashMap<>();
        long inferredFieldCount = mergeTypes(firstRecordTypes, inferences, 0);
        do {
            final RecordState recordState = createRecordState(parser, metadataCollector);
            inferredFieldCount = mergeTypes(readObjectTypes(parser, recordState), inferences, inferredFieldCount);
            addRecordMetadata(metadataCollector, parser, recordState);
        } while ((parser = recordSource.next()) != null);

        return inferredSchema(defaultArrayTypes(createSchema(inferences, null)), metadataCollector);
    }

    private Map<String, DataType> readObjectTypes(final JsonParser parser, final RecordState recordState) throws IOException {
        final Map<String, DataType> fieldTypes = new LinkedHashMap<>();
        long inferredFieldCount = 0;
        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == null) {
                throw new IOException("Unexpected end of JSON while inferring an object schema");
            }
            if (token != JsonToken.FIELD_NAME) {
                throw new IOException("Expected a JSON field name but found " + token);
            }

            if (recordState != null) {
                recordState.hasObjectMembers = true;
            }
            final String fieldName = parser.currentName();
            final JsonToken valueToken = parser.nextToken();
            if (valueToken == null) {
                throw new IOException("Unexpected end of JSON while inferring field " + fieldName);
            }
            final int fieldCount = fieldTypes.size();
            if (fieldCount == maximumSchemaFields && !fieldTypes.containsKey(fieldName)) {
                throw new IOException("JSON object exceeds the schema inference field limit of " + maximumSchemaFields);
            }
            final DataType inferredType = inferType(parser, valueToken, recordState);
            final DataType previousType = fieldTypes.put(fieldName, inferredType);
            final boolean fieldAdded = fieldTypes.size() != fieldCount;
            if (fieldAdded) {
                inferredFieldCount += 1 + countInferredFields(inferredType);
            } else if (previousType == null ? inferredType != null : !previousType.equals(inferredType)) {
                inferredFieldCount += countInferredFields(inferredType) - countInferredFields(previousType);
            }
            validateInferredFieldCount(inferredFieldCount);
            if (recordState != null && !fieldAdded) {
                recordState.containsDuplicateFields = true;
            }
        }
        return fieldTypes;
    }

    private DataType inferType(final JsonParser parser, final JsonToken token, final RecordState recordState) throws IOException {
        return switch (token) {
            case START_OBJECT -> RecordFieldType.RECORD.getRecordDataType(createSchemaFromTypes(readObjectTypes(parser, recordState)));
            case START_ARRAY -> inferArrayType(parser, recordState);
            case VALUE_STRING -> inferStringType(parser.getText());
            case VALUE_NUMBER_INT -> inferIntegerType(parser.getNumberType());
            case VALUE_NUMBER_FLOAT -> {
                if (recordState != null) {
                    recordState.containsScientificNotation |= hasScientificNotation(parser);
                }
                yield RecordFieldType.DOUBLE.getDataType();
            }
            case VALUE_TRUE, VALUE_FALSE -> RecordFieldType.BOOLEAN.getDataType();
            case VALUE_EMBEDDED_OBJECT -> parser.getEmbeddedObject() instanceof byte[]
                    ? RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()) : null;
            case VALUE_NULL -> null;
            default -> throw new IOException("Unsupported JSON token while inferring a value: " + token);
        };
    }

    private DataType inferArrayType(final JsonParser parser, final RecordState recordState) throws IOException {
        final FieldTypeInference elementInference = new FieldTypeInference();
        JsonToken token = parser.nextToken();
        if (token == JsonToken.END_ARRAY) {
            return RecordFieldType.ARRAY.getArrayDataType(null);
        }

        while (token != JsonToken.END_ARRAY) {
            if (token == null) {
                throw new IOException("Unexpected end of JSON while inferring an array schema");
            }
            if (elementInference.addPossibleDataType(inferType(parser, token, recordState))) {
                final long nestedFieldCount = countInferredFields(elementInference.toDataType());
                validateInferredFieldCount(nestedFieldCount);
            }
            token = parser.nextToken();
        }
        return RecordFieldType.ARRAY.getArrayDataType(elementInference.toDataType());
    }

    private DataType inferStringType(final String value) {
        final Optional<DataType> timeDataType = timeValueInference.getDataType(value);
        return timeDataType.orElse(RecordFieldType.STRING.getDataType());
    }

    private DataType inferIntegerType(final NumberType numberType) {
        return switch (numberType) {
            case BIG_INTEGER -> RecordFieldType.BIGINT.getDataType();
            case LONG -> RecordFieldType.LONG.getDataType();
            default -> RecordFieldType.INT.getDataType();
        };
    }

    private RecordSchema createSchemaFromTypes(final Map<String, DataType> fieldTypes) {
        final List<RecordField> fields = new ArrayList<>(fieldTypes.size());
        fieldTypes.forEach((fieldName, dataType) -> fields.add(new RecordField(fieldName,
                dataType == null ? RecordFieldType.STRING.getDataType() : dataType)));
        return new SimpleRecordSchema(fields);
    }

    private long mergeTypes(final Map<String, DataType> fieldTypes, final Map<String, FieldTypeInference> inferences,
            long inferredFieldCount) throws IOException {
        for (final Map.Entry<String, DataType> entry : fieldTypes.entrySet()) {
            final String fieldName = entry.getKey();
            FieldTypeInference inference = inferences.get(fieldName);
            final boolean fieldAdded = inference == null;
            if (inference == null) {
                inference = new FieldTypeInference();
                inferences.put(fieldName, inference);
            }
            if (fieldAdded) {
                inferredFieldCount++;
            }
            if (inference.addPossibleDataType(entry.getValue())) {
                final long previousNestedFieldCount = inference.getNestedFieldCount();
                final long nestedFieldCount = countInferredFields(inference.toDataType());
                inference.setNestedFieldCount(nestedFieldCount);
                inferredFieldCount += nestedFieldCount - previousNestedFieldCount;
            }
            validateInferredFieldCount(inferredFieldCount);
        }
        return inferredFieldCount;
    }

    private long countInferredFields(final DataType dataType) throws IOException {
        if (dataType == null) {
            return 0;
        }
        return switch (dataType.getFieldType()) {
            case RECORD -> {
                final RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                long fieldCount = 0;
                for (final RecordField field : childSchema.getFields()) {
                    fieldCount++;
                    fieldCount += countInferredFields(field.getDataType());
                    validateInferredFieldCount(fieldCount);
                }
                yield fieldCount;
            }
            case ARRAY -> countInferredFields(((ArrayDataType) dataType).getElementType());
            case MAP -> countInferredFields(((MapDataType) dataType).getValueType());
            case CHOICE -> {
                long fieldCount = 0;
                for (final DataType possibleType : ((ChoiceDataType) dataType).getPossibleSubTypes()) {
                    fieldCount += countInferredFields(possibleType);
                    validateInferredFieldCount(fieldCount);
                }
                yield fieldCount;
            }
            default -> 0;
        };
    }

    private void validateInferredFieldCount(final long fieldCount) throws IOException {
        if (fieldCount > maximumSchemaFields) {
            throw new IOException("Inferred JSON schema exceeds the field limit of " + maximumSchemaFields);
        }
    }

    private boolean hasScientificNotation(final JsonParser parser) throws IOException {
        final char[] characters = parser.getTextCharacters();
        final int start = parser.getTextOffset();
        final int end = start + parser.getTextLength();
        for (int i = start; i < end; i++) {
            if (characters[i] == 'e' || characters[i] == 'E') {
                return true;
            }
        }
        return false;
    }

    private RecordState createRecordState(final JsonParser parser, final MetadataCollector metadataCollector) {
        return metadataCollector == null || !metadataCollector.prepareForRecord() ? null : new RecordState(parser);
    }

    private void addRecordMetadata(final MetadataCollector metadataCollector, final JsonParser parser, final RecordState recordState) {
        if (metadataCollector == null || !metadataCollector.complete || recordState == null) {
            return;
        }

        final JsonLocation endLocation = parser.currentLocation();
        final long endOffset = endLocation.getByteOffset();
        final int endLine = endLocation.getLineNr();
        metadataCollector.collect(new JsonRecordMetadata(recordState.startOffset, endOffset, recordState.startLine != endLine,
                recordState.containsScientificNotation, recordState.hasObjectMembers, recordState.containsDuplicateFields));
    }

    private InferredJsonSchema inferredSchema(final RecordSchema schema, final MetadataCollector metadataCollector) {
        if (metadataCollector == null) {
            return new InferredJsonSchema(schema, List.of(), true);
        }
        return metadataCollector.complete
                ? new InferredJsonSchema(schema, metadataCollector.records(), true)
                : new InferredJsonSchema(schema, List.of(), false);
    }

    record InferredJsonSchema(RecordSchema schema, List<JsonRecordMetadata> records, boolean metadataComplete) {
    }

    record JsonRecordMetadata(long startOffset, long endOffset, boolean containsLineBreak,
                              boolean containsScientificNotation, boolean hasObjectMembers, boolean containsDuplicateFields) {
    }

    private static final class RecordState {
        private final long startOffset;
        private final int startLine;
        private boolean containsScientificNotation;
        private boolean hasObjectMembers;
        private boolean containsDuplicateFields;

        private RecordState(final JsonParser parser) {
            final JsonLocation startLocation = parser.currentTokenLocation();
            startOffset = startLocation.getByteOffset();
            startLine = startLocation.getLineNr();
        }
    }

    private static final class MetadataCollector extends AbstractList<JsonRecordMetadata> {
        private final int maximumRecords;
        private JsonRecordMetadata firstRecord;
        private List<JsonRecordMetadata> records;
        private boolean complete = true;

        private MetadataCollector(final int maximumRecords) {
            if (maximumRecords < 0) {
                throw new IllegalArgumentException("Maximum records must not be negative");
            }
            this.maximumRecords = maximumRecords;
        }

        private void collect(final JsonRecordMetadata metadata) {
            if (!complete) {
                return;
            }

            if (firstRecord == null) {
                firstRecord = metadata;
            } else {
                if (records == null) {
                    records = new ArrayList<>();
                    records.add(firstRecord);
                }
                records.add(metadata);
            }
        }

        private boolean prepareForRecord() {
            if (!complete) {
                return false;
            }

            final int recordCount = firstRecord == null ? 0 : records == null ? 1 : records.size();
            if (recordCount >= maximumRecords) {
                firstRecord = null;
                records = null;
                complete = false;
                return false;
            }
            return true;
        }

        private List<JsonRecordMetadata> records() {
            return this;
        }

        @Override
        public JsonRecordMetadata get(final int index) {
            if (records != null) {
                return records.get(index);
            }
            if (index == 0 && firstRecord != null) {
                return firstRecord;
            }
            throw new IndexOutOfBoundsException(index);
        }

        @Override
        public int size() {
            return firstRecord == null ? 0 : records == null ? 1 : records.size();
        }
    }
}
