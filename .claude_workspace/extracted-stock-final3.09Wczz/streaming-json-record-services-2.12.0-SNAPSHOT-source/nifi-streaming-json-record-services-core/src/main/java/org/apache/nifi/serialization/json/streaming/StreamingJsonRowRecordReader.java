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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

final class StreamingJsonRowRecordReader implements RecordReader {
    private static final String MIME_TYPE = "application/json";
    private static final int MAX_SCHEMA_DEPTH = 100;
    private static final Object[] EMPTY_VALUES = new Object[0];
    private static final RecordSchema EMPTY_SCHEMA = new SimpleRecordSchema(List.of());

    private final byte[] input;
    private final int inputOffset;
    private final int inputLength;
    private final ComponentLog logger;
    private final RecordSchema schema;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final Optional<String> dateFormatOption;
    private final Optional<String> timeFormatOption;
    private final Optional<String> timestampFormatOption;
    private final StartingFieldStrategy startingFieldStrategy;
    private final String startingFieldName;
    private final TokenParserFactory tokenParserFactory;
    private final JsonParser parser;
    private final RecordCapturingInputStream inputCapture;
    private final InputStream sourceInput;
    private final SchemaMutationSnapshot schemaMutationSnapshot;
    private final boolean utf8Input;

    private boolean skippedToStartField;
    private boolean serializedFormReusable;
    private boolean serializedFormNormalizationRequired;
    private boolean parserCloseAttempted;
    private boolean sourceCloseComplete;

    StreamingJsonRowRecordReader(
            final InputStream input,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final SchemaApplicationStrategy schemaApplicationStrategy,
            final TokenParserFactory tokenParserFactory) throws IOException {
        this(input, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, tokenParserFactory, true);
    }

    StreamingJsonRowRecordReader(
            final InputStream input,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final SchemaApplicationStrategy schemaApplicationStrategy,
            final TokenParserFactory tokenParserFactory,
            final boolean captureSerializedForm) throws IOException {
        this.input = null;
        this.inputOffset = 0;
        this.inputLength = 0;
        this.sourceInput = input;
        this.logger = logger;
        this.schema = startingFieldStrategy == StartingFieldStrategy.NESTED_FIELD && schemaApplicationStrategy == SchemaApplicationStrategy.WHOLE_JSON
                ? JsonSchemaSelection.select(schema, startingFieldName)
                : schema;
        this.schemaMutationSnapshot = SchemaMutationSnapshot.capture(this.schema);
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        dateFormatOption = Optional.ofNullable(dateFormat);
        timeFormatOption = Optional.ofNullable(timeFormat);
        timestampFormatOption = Optional.ofNullable(timestampFormat);
        this.startingFieldStrategy = startingFieldStrategy;
        this.startingFieldName = startingFieldName;
        this.tokenParserFactory = tokenParserFactory;
        this.utf8Input = false;
        this.inputCapture = captureSerializedForm && tokenParserFactory.supportsSerializedJson() ? new RecordCapturingInputStream(input) : null;
        this.parser = tokenParserFactory.getJsonParser(inputCapture == null ? input : inputCapture);
        parser.enable(Feature.USE_FAST_DOUBLE_PARSER);
        parser.enable(Feature.USE_FAST_BIG_NUMBER_PARSER);
    }

    StreamingJsonRowRecordReader(
            final byte[] input,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final SchemaApplicationStrategy schemaApplicationStrategy,
            final TokenParserFactory tokenParserFactory) throws IOException {
        this(input, 0, input.length, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy,
                startingFieldName, schemaApplicationStrategy, tokenParserFactory);
    }

    StreamingJsonRowRecordReader(
            final byte[] input,
            final int inputOffset,
            final int inputLength,
            final ComponentLog logger,
            final RecordSchema schema,
            final String dateFormat,
            final String timeFormat,
            final String timestampFormat,
            final StartingFieldStrategy startingFieldStrategy,
            final String startingFieldName,
            final SchemaApplicationStrategy schemaApplicationStrategy,
            final TokenParserFactory tokenParserFactory) throws IOException {
        Objects.checkFromIndexSize(inputOffset, inputLength, input.length);
        this.input = input;
        this.inputOffset = inputOffset;
        this.inputLength = inputLength;
        this.sourceInput = null;
        this.logger = logger;
        this.schema = startingFieldStrategy == StartingFieldStrategy.NESTED_FIELD && schemaApplicationStrategy == SchemaApplicationStrategy.WHOLE_JSON
                ? JsonSchemaSelection.select(schema, startingFieldName)
                : schema;
        this.schemaMutationSnapshot = SchemaMutationSnapshot.capture(this.schema);
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        dateFormatOption = Optional.ofNullable(dateFormat);
        timeFormatOption = Optional.ofNullable(timeFormat);
        timestampFormatOption = Optional.ofNullable(timestampFormat);
        this.startingFieldStrategy = startingFieldStrategy;
        this.startingFieldName = startingFieldName;
        this.tokenParserFactory = tokenParserFactory;
        this.inputCapture = null;
        utf8Input = Utf8JsonValue.isUtf8EncodedJson(input, inputOffset, inputLength);
        parser = tokenParserFactory.getJsonParser(input, inputOffset, inputLength);
        parser.enable(Feature.USE_FAST_DOUBLE_PARSER);
        parser.enable(Feature.USE_FAST_BIG_NUMBER_PARSER);
    }

    static boolean isSchemaSupported(final RecordSchema schema) {
        return isSchemaSupported(schema, 0);
    }

    private static boolean isSchemaSupported(final RecordSchema schema, final int depth) {
        if (schema == null || depth > MAX_SCHEMA_DEPTH) {
            return false;
        }

        for (final RecordField field : schema.getFields()) {
            if (!isTypeSupported(field.getDataType(), depth)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTypeSupported(final DataType dataType, final int depth) {
        if (dataType == null || depth > MAX_SCHEMA_DEPTH) {
            return false;
        }

        return switch (dataType.getFieldType()) {
            case CHOICE -> false;
            case ARRAY -> isTypeSupported(((ArrayDataType) dataType).getElementType(), depth + 1);
            case MAP -> isTypeSupported(((MapDataType) dataType).getValueType(), depth + 1);
            case RECORD -> isSchemaSupported(((RecordDataType) dataType).getChildSchema(), depth + 1);
            default -> true;
        };
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        try {
            while (true) {
                final JsonToken token = parser.nextToken();
                if (token == null) {
                    return null;
                }

                switch (token) {
                    case START_ARRAY:
                        break;
                    case END_ARRAY:
                    case END_OBJECT:
                        skippedToStartField = false;
                        break;
                    case START_OBJECT:
                        if (startingFieldStrategy == StartingFieldStrategy.NESTED_FIELD && !skippedToStartField) {
                            skipToStartingField();
                            skippedToStartField = true;
                            break;
                        }

                        serializedFormReusable = utf8Input || inputCapture != null;
                        serializedFormNormalizationRequired = false;
                        if (input == null && inputCapture == null) {
                            return readRecord(schema, coerceTypes, dropUnknownFields, false, -1, -1);
                        }

                        final JsonLocation startLocation = parser.currentTokenLocation();
                        final long startOffset = startLocation.getByteOffset();
                        final int startLine = startLocation.getLineNr();
                        final long capturedStartOffset = inputCapture != null && !inputCapture.startRecord(startOffset) ? -1 : startOffset;
                        final JsonStreamContext parentContext = parser.getParsingContext().getParent();
                        try {
                            return readRecord(schema, coerceTypes, dropUnknownFields, true, capturedStartOffset, startLine);
                        } catch (final RuntimeException conversionFailure) {
                            final long endOffset = skipToRecordEnd(parentContext);
                            if (input == null && inputCapture == null) {
                                throw conversionFailure;
                            }
                            if (inputCapture != null && (capturedStartOffset < 0 || inputCapture.isRecordCaptureExceeded())) {
                                if (inputCapture.isRecordCaptureExceeded()) {
                                    inputCapture.finishRecord(endOffset);
                                }
                                throw conversionFailure;
                            }
                            try {
                                return readTreeRecord(schema, coerceTypes, dropUnknownFields, capturedStartOffset, endOffset);
                            } catch (final Exception treeFailure) {
                                if (treeFailure != conversionFailure) {
                                    treeFailure.addSuppressed(conversionFailure);
                                }
                                throw treeFailure;
                            }
                        }
                    default:
                        break;
                }
            }
        } catch (final MalformedRecordException e) {
            throw e;
        } catch (final JsonParseException e) {
            throw new MalformedRecordException("Failed to parse JSON", e);
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            logger.debug("Failed to convert JSON into a Record object using schema {}", schema, e);
            throw new MalformedRecordException("Successfully parsed JSON input but failed to convert it using the configured schema", e);
        }
    }

    private Record readRecord(final RecordSchema recordSchema, final boolean coerceTypes, final boolean dropUnknownFields,
                              final boolean useTreeFallback, final long startOffset, final int startLine)
            throws IOException, MalformedRecordException {
        final Map<String, Object> values = new LinkedHashMap<>(recordSchema.getFieldCount() * 2);
        Map<String, Integer> fieldPriorities = null;
        Map<String, RuntimeException> conversionFailures = null;
        final JsonStreamContext recordContext = parser.getParsingContext();

        JsonToken token;
        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == null) {
                throw unexpectedEnd(JsonToken.START_OBJECT, "object");
            }
            if (token != JsonToken.FIELD_NAME) {
                throw new MalformedRecordException("Expected a JSON field name but found " + token);
            }

            final String inputFieldName = parser.currentName();
            final RecordField field = recordSchema.getField(inputFieldName).orElse(null);
            final JsonToken valueToken = parser.nextToken();
            if (valueToken == null) {
                throw unexpectedEnd(JsonToken.FIELD_NAME, "field " + inputFieldName);
            }

            if (field == null && dropUnknownFields) {
                parser.skipChildren();
                serializedFormReusable = false;
                continue;
            }

            final DataType dataType = field == null ? null : field.getDataType();
            Object value;
            RuntimeException conversionFailure = null;
            try {
                value = readValue(valueToken, dataType, inputFieldName, coerceTypes, dropUnknownFields);
            } catch (final RuntimeException e) {
                skipToContext(recordContext);
                value = null;
                conversionFailure = e;
            }
            if (dropUnknownFields) {
                final String fieldName = field.getFieldName();
                if (fieldName.equals(inputFieldName)) {
                    final int fieldCount = values.size();
                    values.put(fieldName, value);
                    serializedFormNormalizationRequired |= values.size() == fieldCount;
                    if (fieldPriorities != null) {
                        fieldPriorities.put(fieldName, 0);
                    }
                    conversionFailures = updateConversionFailure(conversionFailures, fieldName, conversionFailure);
                } else {
                    if (fieldPriorities == null) {
                        fieldPriorities = new HashMap<>();
                    }
                    final int priority = getFieldPriority(field, inputFieldName);
                    Integer existingPriority = fieldPriorities.get(fieldName);
                    if (existingPriority == null && values.containsKey(fieldName)) {
                        existingPriority = 0;
                        fieldPriorities.put(fieldName, existingPriority);
                    }
                    if (existingPriority != null && priority == existingPriority) {
                        serializedFormNormalizationRequired = true;
                    }
                    if (existingPriority == null || priority <= existingPriority) {
                        values.put(fieldName, value);
                        fieldPriorities.put(fieldName, priority);
                        conversionFailures = updateConversionFailure(conversionFailures, fieldName, conversionFailure);
                    }
                }
            } else {
                final int fieldCount = values.size();
                values.put(inputFieldName, value);
                serializedFormNormalizationRequired |= values.size() == fieldCount;
                conversionFailures = updateConversionFailure(conversionFailures, inputFieldName, conversionFailure);
            }
        }

        if (conversionFailures != null && !conversionFailures.isEmpty()) {
            throw conversionFailures.values().iterator().next();
        }

        SerializedForm serializedForm = null;
        if (input != null) {
            final JsonLocation endLocation = parser.currentLocation();
            final long endOffset = endLocation.getByteOffset();
            final int endLine = endLocation.getLineNr();
            if (isValidParserRange(startOffset, endOffset)) {
                if (useTreeFallback && serializedFormReusable && !serializedFormNormalizationRequired) {
                    final int offset = toAbsoluteOffset(startOffset);
                    final int length = Math.toIntExact(endOffset - startOffset);
                    final Utf8JsonValue value = startLine > 0 && endLine > 0
                            ? new Utf8JsonValue(input, offset, length, startLine != endLine)
                            : new Utf8JsonValue(input, offset, length);
                    serializedForm = SerializedForm.of(value, MIME_TYPE);
                } else if (serializedFormReusable) {
                    serializedForm = createNormalizedSerializedForm(toAbsoluteOffset(startOffset), Math.toIntExact(endOffset - startOffset));
                }
            }
        } else if (inputCapture != null && startOffset >= 0) {
            final JsonLocation endLocation = parser.currentLocation();
            final long endOffset = endLocation.getByteOffset();
            final boolean captured = inputCapture.contains(startOffset, endOffset);
            if (!captured && !inputCapture.isRecordCaptureExceeded()) {
                throw new MalformedRecordException("Invalid JSON record byte offsets");
            }
            if (captured && serializedFormReusable && !serializedFormNormalizationRequired) {
                final byte[] recordBytes = inputCapture.copyRange(startOffset, endOffset);
                if (Utf8JsonValue.isUtf8EncodedJson(recordBytes)) {
                    final int endLine = endLocation.getLineNr();
                    final Utf8JsonValue value = startLine > 0 && endLine > 0
                            ? new Utf8JsonValue(recordBytes, 0, recordBytes.length, startLine != endLine)
                            : new Utf8JsonValue(recordBytes);
                    serializedForm = SerializedForm.of(value, MIME_TYPE);
                }
            }
            inputCapture.finishRecord(endOffset);
        }
        final MapRecord materializedRecord = new MapRecord(recordSchema, values, serializedForm, false, dropUnknownFields);
        return serializedForm == null ? materializedRecord
                : new DeferredJsonRecord(recordSchema, coerceTypes, dropUnknownFields, serializedForm,
                        () -> materializedRecord, schemaMutationSnapshot, false);
    }

    private int getFieldPriority(final RecordField field, final String inputFieldName) {
        if (field.getFieldName().equals(inputFieldName)) {
            return 0;
        }

        int priority = 1;
        for (final String alias : field.getAliases()) {
            if (alias.equals(inputFieldName)) {
                return priority;
            }
            priority++;
        }
        return Integer.MAX_VALUE;
    }

    private Map<String, RuntimeException> updateConversionFailure(final Map<String, RuntimeException> failures, final String fieldName,
                                                                  final RuntimeException failure) {
        if (failure == null) {
            if (failures != null) {
                failures.remove(fieldName);
            }
            return failures;
        }

        final Map<String, RuntimeException> updatedFailures = failures == null ? new LinkedHashMap<>() : failures;
        updatedFailures.put(fieldName, failure);
        return updatedFailures;
    }

    private Object readValue(final JsonToken token, final DataType dataType, final String fieldName,
                             final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (token == JsonToken.VALUE_NULL) {
            return null;
        }
        if (!coerceTypes || dataType == null) {
            return readRawValue(token, dataType, fieldName);
        }

        return switch (dataType.getFieldType()) {
            case ARRAY -> readArray(token, ((ArrayDataType) dataType).getElementType(), fieldName, coerceTypes, dropUnknownFields);
            case MAP -> readMap(token, ((MapDataType) dataType).getValueType(), fieldName, coerceTypes, dropUnknownFields);
            case RECORD -> readRecord(token, ((RecordDataType) dataType).getChildSchema(), coerceTypes, dropUnknownFields);
            case CHOICE -> throw new MalformedRecordException("Streaming reader does not support Choice fields");
            default -> DataTypeUtils.convertType(readTreeCompatibleRawValue(token, fieldName), dataType,
                    dateFormatOption, timeFormatOption, timestampFormatOption, fieldName);
        };
    }

    private Object readTreeCompatibleRawValue(final JsonToken token, final String fieldName) throws IOException, MalformedRecordException {
        if (token != JsonToken.START_ARRAY && token != JsonToken.START_OBJECT) {
            return readRawValue(token, null, fieldName);
        }
        return toRawValue(parser.readValueAsTree());
    }

    private Object toRawValue(final JsonNode node) throws IOException {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBinary()) {
            return node.binaryValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        if (node.isArray()) {
            final Object[] values = new Object[node.size()];
            int index = 0;
            for (final JsonNode element : node) {
                values[index++] = toRawValue(element);
            }
            return values;
        }

        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonNode> entry : node.properties()) {
            values.put(entry.getKey(), toRawValue(entry.getValue()));
        }
        return new MapRecord(EMPTY_SCHEMA, values, SerializedForm.of((Supplier<String>) node::toString, MIME_TYPE));
    }

    private Record readRecord(final JsonToken token, final RecordSchema childSchema,
                              final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (token != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return null;
        }
        if (input == null) {
            return readRecord(childSchema, coerceTypes, dropUnknownFields, false, -1, -1);
        }
        final JsonLocation startLocation = parser.currentTokenLocation();
        return readRecord(childSchema, coerceTypes, dropUnknownFields, false,
                startLocation.getByteOffset(), startLocation.getLineNr());
    }

    private Record readTreeRecord(final RecordSchema recordSchema, final boolean coerceTypes, final boolean dropUnknownFields,
                                  final long startOffset, final long endOffset) throws IOException, MalformedRecordException {
        final byte[] recordInput;
        final int recordOffset;
        final int recordLength;
        if (input != null && isValidParserRange(startOffset, endOffset)) {
            recordInput = input;
            recordOffset = toAbsoluteOffset(startOffset);
            recordLength = Math.toIntExact(endOffset - startOffset);
        } else if (inputCapture != null && inputCapture.contains(startOffset, endOffset)) {
            recordInput = inputCapture.copyRange(startOffset, endOffset);
            recordOffset = 0;
            recordLength = recordInput.length;
            inputCapture.finishRecord(endOffset);
        } else {
            throw new MalformedRecordException("Invalid JSON record byte offsets");
        }

        try (final JsonTreeRowRecordReader treeReader = new JsonTreeRowRecordReader(new ByteArrayInputStream(recordInput, recordOffset, recordLength), logger, recordSchema,
                dateFormat, timeFormat, timestampFormat, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, null, tokenParserFactory)) {
            return treeReader.nextRecord(coerceTypes, dropUnknownFields);
        }
    }

    private Object[] readArray(final JsonToken token, final DataType elementType, final String fieldName,
                               final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (token != JsonToken.START_ARRAY) {
            // Consume the mismatched value before reproducing the legacy tree conversion failure.
            final ArrayNode ignored = (ArrayNode) parser.readValueAsTree();
            return new Object[ignored.size()];
        }

        Object[] values = EMPTY_VALUES;
        int count = 0;
        JsonToken elementToken;
        while ((elementToken = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (elementToken == null) {
                throw unexpectedEnd(JsonToken.START_ARRAY, "array for field " + fieldName);
            }
            if (count == values.length) {
                values = Arrays.copyOf(values, values.length == 0 ? 8 : values.length << 1);
            }
            values[count++] = readValue(elementToken, elementType, fieldName, coerceTypes, dropUnknownFields);
        }
        return count == values.length ? values : Arrays.copyOf(values, count);
    }

    private Map<String, Object> readMap(final JsonToken token, final DataType valueType, final String fieldName,
                                        final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (token != JsonToken.START_OBJECT) {
            parser.skipChildren();
            return new LinkedHashMap<>();
        }

        final Map<String, Object> values = new LinkedHashMap<>();
        JsonToken fieldToken;
        while ((fieldToken = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (fieldToken != JsonToken.FIELD_NAME) {
                throw new MalformedRecordException("Expected a map key for field " + fieldName);
            }
            final String key = parser.currentName();
            final JsonToken valueToken = parser.nextToken();
            final int fieldCount = values.size();
            values.put(key, readValue(valueToken, valueType, fieldName, coerceTypes, dropUnknownFields));
            serializedFormNormalizationRequired |= values.size() == fieldCount;
        }
        return values;
    }

    private Object readRawValue(final JsonToken token, final DataType dataType, final String fieldName) throws IOException, MalformedRecordException {
        return switch (token) {
            case START_ARRAY -> readRawArray(dataType, fieldName);
            case START_OBJECT -> readRawObject(dataType, fieldName);
            default -> convertRawScalar(readScalar(token), dataType, fieldName);
        };
    }

    private Object[] readRawArray(final DataType dataType, final String fieldName) throws IOException, MalformedRecordException {
        final DataType elementType = dataType instanceof final ArrayDataType arrayType ? arrayType.getElementType() : dataType;
        Object[] values = EMPTY_VALUES;
        int count = 0;
        JsonToken elementToken;
        while ((elementToken = parser.nextToken()) != JsonToken.END_ARRAY) {
            if (elementToken == null) {
                throw new MalformedRecordException("Unexpected end of array for field " + fieldName);
            }
            if (count == values.length) {
                values = Arrays.copyOf(values, values.length == 0 ? 8 : values.length << 1);
            }
            values[count++] = readRawValue(elementToken, elementType, fieldName);
        }
        return count == values.length ? values : Arrays.copyOf(values, count);
    }

    private Object readRawObject(final DataType dataType, final String fieldName) throws IOException, MalformedRecordException {
        if (dataType instanceof final MapDataType mapType) {
            return readRawMap(mapType.getValueType(), fieldName);
        }

        final RecordSchema childSchema = dataType instanceof final RecordDataType recordType && recordType.getChildSchema() != null
                ? recordType.getChildSchema()
                : EMPTY_SCHEMA;
        return readRawRecord(childSchema, fieldName);
    }

    private Map<String, Object> readRawMap(final DataType valueType, final String fieldName) throws IOException, MalformedRecordException {
        final Map<String, Object> values = new LinkedHashMap<>();
        JsonToken fieldToken;
        while ((fieldToken = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (fieldToken == null) {
                throw unexpectedEnd(JsonToken.START_OBJECT, "object for " + fieldName);
            }
            if (fieldToken != JsonToken.FIELD_NAME) {
                throw new MalformedRecordException("Expected an object field for " + fieldName);
            }
            final String key = parser.currentName();
            final int fieldCount = values.size();
            values.put(key, readRawValue(parser.nextToken(), valueType, fieldName + "['" + key + "']"));
            serializedFormNormalizationRequired |= values.size() == fieldCount;
        }
        return values;
    }

    private Record readRawRecord(final RecordSchema childSchema, final String fieldName) throws IOException, MalformedRecordException {
        final long startOffset = input == null ? -1 : parser.currentTokenLocation().getByteOffset();
        final Map<String, Object> values = new LinkedHashMap<>();
        JsonToken fieldToken;
        while ((fieldToken = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (fieldToken == null) {
                throw unexpectedEnd(JsonToken.START_OBJECT, "object for " + fieldName);
            }
            if (fieldToken != JsonToken.FIELD_NAME) {
                throw new MalformedRecordException("Expected an object field for " + fieldName);
            }
            final String key = parser.currentName();
            final int fieldCount = values.size();
            values.put(key, readRawValue(parser.nextToken(), childSchema.getDataType(key).orElse(null), key));
            serializedFormNormalizationRequired |= values.size() == fieldCount;
        }

        final SerializedForm serializedForm;
        if (input == null) {
            serializedForm = null;
        } else {
            final long endOffset = parser.currentLocation().getByteOffset();
            serializedForm = !isValidParserRange(startOffset, endOffset)
                    ? null
                    : createNormalizedSerializedForm(toAbsoluteOffset(startOffset), Math.toIntExact(endOffset - startOffset));
        }
        return new MapRecord(childSchema, values, serializedForm);
    }

    private SerializedForm createNormalizedSerializedForm(final int offset, final int length) {
        final byte[] source = input;
        final TokenParserFactory parserFactory = tokenParserFactory;
        return SerializedForm.of((Supplier<String>) () -> normalizeJson(parserFactory, source, offset, length), MIME_TYPE);
    }

    private static String normalizeJson(final TokenParserFactory parserFactory, final byte[] source, final int offset, final int length) {
        try (final JsonParser nestedParser = parserFactory.getJsonParser(source, offset, length)) {
            return nestedParser.readValueAsTree().toString();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Object readScalar(final JsonToken token) throws IOException, MalformedRecordException {
        return switch (token) {
            case VALUE_STRING -> parser.getText();
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> parser.getNumberValue();
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_EMBEDDED_OBJECT -> parser.getBinaryValue();
            case VALUE_NULL -> null;
            default -> throw new MalformedRecordException("Expected a scalar JSON value but found " + token);
        };
    }

    private Object convertRawScalar(final Object value, final DataType dataType, final String fieldName) {
        if (dataType == null) {
            return value;
        }
        if (value instanceof final String stringValue && (dataType.getFieldType() == RecordFieldType.DATE
                || dataType.getFieldType() == RecordFieldType.TIME || dataType.getFieldType() == RecordFieldType.TIMESTAMP)) {
            try {
                return DataTypeUtils.convertType(stringValue, dataType, dateFormatOption, timeFormatOption, timestampFormatOption, fieldName);
            } catch (final Exception ignored) {
                return stringValue;
            }
        }
        return value;
    }

    private void skipToStartingField() throws IOException {
        while (parser.nextToken() != null) {
            if (startingFieldName.equals(parser.currentName())) {
                break;
            }
        }
    }

    private long skipToRecordEnd(final JsonStreamContext parentContext) throws IOException, MalformedRecordException {
        skipToContext(parentContext);
        return parser.currentLocation().getByteOffset();
    }

    private void skipToContext(final JsonStreamContext context) throws IOException, MalformedRecordException {
        while (parser.getParsingContext() != context) {
            if (parser.nextToken() == null) {
                throw unexpectedEnd(JsonToken.START_OBJECT, "object");
            }
        }
    }

    private JsonEOFException unexpectedEnd(final JsonToken token, final String structure) {
        return new JsonEOFException(parser, token, "Unexpected end-of-input while reading JSON " + structure);
    }

    private boolean isValidParserRange(final long startOffset, final long endOffset) {
        return input != null && startOffset >= 0 && endOffset >= startOffset && endOffset <= inputLength;
    }

    private int toAbsoluteOffset(final long parserOffset) {
        return Math.addExact(inputOffset, Math.toIntExact(parserOffset));
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {
        if (!parserCloseAttempted) {
            parserCloseAttempted = true;
            parser.close();
            sourceCloseComplete = true;
        } else if (!sourceCloseComplete && sourceInput != null) {
            sourceInput.close();
            sourceCloseComplete = true;
        }
    }
}
