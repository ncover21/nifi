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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessWriter;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RawRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

final class StreamingJsonWriteResult extends AbstractRecordSetWriter implements RecordSetWriter, RawRecordWriter {
    private static final String JSON_MIME_TYPE = "application/json";
    private static final int MAX_COMPATIBLE_SERIALIZED_SCHEMAS = 64;
    private static final FieldConverter<Object, String> STRING_FIELD_CONVERTER = StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class);
    private static final Pattern SCIENTIFIC_NOTATION_PATTERN = Pattern.compile("[0-9]([eE][-+]?)[0-9]");

    private final ComponentLog logger;
    private final SchemaAccessWriter schemaAccess;
    private final RecordSchema recordSchema;
    private final JsonGenerator generator;
    private final NullSuppression nullSuppression;
    private final OutputGrouping outputGrouping;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final Optional<String> dateFormatOption;
    private final Optional<String> timeFormatOption;
    private final Optional<String> timestampFormatOption;
    private final String mimeType;
    private final boolean prettyPrint;
    private final boolean allowScientificNotation;
    private final boolean serializedInputHandlingEnabled;
    private final TimestampRepresentation timestampRepresentation;
    private final SchemaMutationSnapshot writerSchemaMutationSnapshot;
    private List<SerializedSchemaCompatibility> serializedSchemaCompatibilities;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    StreamingJsonWriteResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
            final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException {
        this(logger, recordSchema, schemaAccess, out, prettyPrint, nullSuppression, outputGrouping, dateFormat, timeFormat, timestampFormat, "application/json", false, true,
                TimestampRepresentation.AUTO);
    }

    StreamingJsonWriteResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
        final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat,
        final String mimeType, final boolean allowScientificNotation) throws IOException {
        this(logger, recordSchema, schemaAccess, out, prettyPrint, nullSuppression, outputGrouping, dateFormat, timeFormat, timestampFormat, mimeType, allowScientificNotation, true,
                TimestampRepresentation.AUTO);
    }

    StreamingJsonWriteResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
        final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat,
        final String mimeType, final boolean allowScientificNotation, final boolean serializedInputHandlingEnabled) throws IOException {
        this(logger, recordSchema, schemaAccess, out, prettyPrint, nullSuppression, outputGrouping, dateFormat, timeFormat, timestampFormat, mimeType, allowScientificNotation,
                serializedInputHandlingEnabled, TimestampRepresentation.AUTO);
    }

    StreamingJsonWriteResult(final ComponentLog logger, final RecordSchema recordSchema, final SchemaAccessWriter schemaAccess, final OutputStream out, final boolean prettyPrint,
        final NullSuppression nullSuppression, final OutputGrouping outputGrouping, final String dateFormat, final String timeFormat, final String timestampFormat,
        final String mimeType, final boolean allowScientificNotation, final boolean serializedInputHandlingEnabled,
        final TimestampRepresentation timestampRepresentation) throws IOException {

        super(out);
        this.logger = logger;
        this.recordSchema = recordSchema;
        this.schemaAccess = schemaAccess;
        this.nullSuppression = nullSuppression;
        this.outputGrouping = outputGrouping;
        this.mimeType = mimeType;
        this.allowScientificNotation = allowScientificNotation;
        this.serializedInputHandlingEnabled = serializedInputHandlingEnabled;
        this.timestampRepresentation = timestampRepresentation;
        this.writerSchemaMutationSnapshot = SchemaMutationSnapshot.capture(recordSchema);

        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        dateFormatOption = Optional.ofNullable(dateFormat);
        timeFormatOption = Optional.ofNullable(timeFormat);
        timestampFormatOption = Optional.ofNullable(timestampFormat);

        this.generator = objectMapper.getFactory().createGenerator(out);
        if (!allowScientificNotation) {
            generator.enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        }

        this.prettyPrint = prettyPrint;
        if (prettyPrint) {
            generator.useDefaultPrettyPrinter();
        } else if (OutputGrouping.OUTPUT_ONELINE.equals(outputGrouping)) {
            // Use a minimal pretty printer with a newline object separator, will output one JSON object per line
            generator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
        }
    }

    @Override
    protected void onBeginRecordSet() throws IOException {
        final OutputStream out = getOutputStream();
        schemaAccess.writeHeader(recordSchema, out);

        if (outputGrouping == OutputGrouping.OUTPUT_ARRAY) {
            generator.writeStartArray();
        }
    }

    @Override
    protected Map<String, String> onFinishRecordSet() throws IOException {
        if (outputGrouping == OutputGrouping.OUTPUT_ARRAY) {
            generator.writeEndArray();
        }
        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public void close() throws IOException {
        if (generator != null) {
            generator.close();
        }

        super.close();
    }

    @Override
    public void flush() throws IOException {
        if (generator != null) {
            generator.flush();
        }
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            generator.flush();
            schemaAccess.writeHeader(recordSchema, getOutputStream());
        }

        writeRecord(record, recordSchema, generator, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, true);
        return schemaAccess.getAttributes(recordSchema);
    }

    @Override
    public WriteResult writeRawRecord(final Record record) throws IOException {
        // If we are not writing an active record set, then we need to ensure that we write the
        // schema information.
        if (!isActiveRecordSet()) {
            generator.flush();
            schemaAccess.writeHeader(recordSchema, getOutputStream());
        }

        writeRecord(record, recordSchema, generator, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, false);
        final Map<String, String> attributes = schemaAccess.getAttributes(recordSchema);
        return WriteResult.of(incrementRecordCount(), attributes);
    }

    /**
     * Determines whether the record's original serialized JSON bytes can be emitted verbatim as a throughput optimization,
     * bypassing field-by-field re-serialization. All of the following conditions must hold for the fast path to apply:
     * <ol>
     *   <li>The caller enabled the optimization (the {@code serializedInputHandlingEnabled} constructor argument is {@code true}).</li>
     *   <li>The record is an unmaterialized {@link DeferredJsonRecord} carrying a JSON {@link SerializedForm} produced by
     *       this bundle's validated byte reader. Materialized or externally supplied records never trigger the fast path.</li>
     *   <li>The serialized form's MIME type is {@code application/json} and the reader's record schema is equal to the
     *       writer's record schema, or merges into the writer schema without changing existing fields.</li>
     *   <li>The cached representation is a {@code String}, UTF-8 {@code byte[]}, or byte-backed JSON value.</li>
     *   <li>The cached bytes' pretty-print state matches the writer's {@code prettyPrint} setting.</li>
     *   <li>If scientific notation is disabled on the writer, the cached bytes do not contain scientific notation.</li>
     * </ol>
     * When the fast path is taken, the writer emits the cached representation as a raw JSON value. Missing top-level
     * fields from a compatible merged schema are appended as null when null suppression is disabled, but the writer does
     * not otherwise apply its Timestamp Format, Date Format, Time Format, or Suppress Null Values settings to that record.
     * Operators that need those writer-side properties to be honored uniformly must construct this writer with
     * {@code serializedInputHandlingEnabled = false}.
     */
    private boolean tryWriteSerializedRecord(final Record record, final RecordSchema writeSchema) throws IOException {
        if (!serializedInputHandlingEnabled || timestampRepresentation != TimestampRepresentation.AUTO) {
            return false;
        }
        if (writeSchema != recordSchema || !writerSchemaMutationSnapshot.isUnmodified()) {
            return false;
        }
        if (!(record instanceof final DeferredJsonRecord deferredRecord)) {
            return false;
        }
        if (!deferredRecord.isSerializedInputSemanticallyEquivalent() || !deferredRecord.isSerializedSchemaUnmodified()) {
            return false;
        }

        final Optional<SerializedForm> serializedForm = deferredRecord.getSerializedForm();
        if (serializedForm.isEmpty()) {
            return false;
        }

        final SerializedForm form = serializedForm.get();
        if (!JSON_MIME_TYPE.equals(form.getMimeType())) {
            return false;
        }
        final SerializedSchemaCompatibility schemaCompatibility = getSerializedSchemaCompatibility(deferredRecord, writeSchema);
        if (!schemaCompatibility.compatible()) {
            return false;
        }

        final Object formValue = form.getSerialized();
        final Object serialized;
        if (formValue instanceof final byte[] bytes) {
            if (!Utf8JsonValue.isUtf8EncodedJson(bytes)) {
                return false;
            }
            serialized = new Utf8JsonValue(bytes);
        } else if (formValue instanceof String || formValue instanceof Utf8JsonValue) {
            serialized = formValue;
        } else {
            return false;
        }
        final boolean serializedPretty = containsLineFeed(serialized);
        if (serializedPretty != this.prettyPrint) {
            return false;
        }

        if (!allowScientificNotation && hasScientificNotation(serialized)) {
            return false;
        }

        if (!schemaCompatibility.missingFields().isEmpty()) {
            if (!(serialized instanceof final Utf8JsonValue utf8JsonValue)) {
                return false;
            }
            writeSerializedObjectWithMissingFields(utf8JsonValue, schemaCompatibility.missingFields());
        } else if (serialized instanceof final String serializedString) {
            generator.writeRawValue(serializedString);
        } else {
            generator.writeRawValue((Utf8JsonValue) serialized);
        }
        return true;
    }

    private SerializedSchemaCompatibility getSerializedSchemaCompatibility(final DeferredJsonRecord record, final RecordSchema writeSchema) {
        final RecordSchema sourceSchema = record.getSchema();
        if (record.isMaterialized()) {
            return SerializedSchemaCompatibility.INCOMPATIBLE;
        }
        if (sourceSchema.equals(writeSchema)) {
            return SerializedSchemaCompatibility.COMPATIBLE;
        }
        if (sourceSchema.isRecursive() || writeSchema.isRecursive()) {
            return SerializedSchemaCompatibility.INCOMPATIBLE;
        }

        if (serializedSchemaCompatibilities != null) {
            for (final SerializedSchemaCompatibility cached : serializedSchemaCompatibilities) {
                if (cached.sourceSchema().equals(sourceSchema)) {
                    return cached;
                }
            }
            if (serializedSchemaCompatibilities.size() == MAX_COMPATIBLE_SERIALIZED_SCHEMAS) {
                return SerializedSchemaCompatibility.INCOMPATIBLE;
            }
        }

        final List<String> missingFields = getMissingFieldsForCompatibleSchema(writeSchema, sourceSchema);
        final boolean missingFieldInjectionUnsupported = prettyPrint && nullSuppression == NullSuppression.NEVER_SUPPRESS
                && missingFields != null && !missingFields.isEmpty();
        final SerializedSchemaCompatibility compatibility = missingFields == null || missingFieldInjectionUnsupported
                ? new SerializedSchemaCompatibility(sourceSchema, false, List.of())
                : new SerializedSchemaCompatibility(sourceSchema, true,
                        nullSuppression == NullSuppression.NEVER_SUPPRESS ? missingFields : List.of());
        if (serializedSchemaCompatibilities == null) {
            serializedSchemaCompatibilities = new ArrayList<>(4);
        }
        serializedSchemaCompatibilities.add(compatibility);
        return compatibility;
    }

    private List<String> getMissingFieldsForCompatibleSchema(final RecordSchema writeSchema, final RecordSchema sourceSchema) {
        if (!DataTypeUtils.isRecordWider(writeSchema, sourceSchema)) {
            return null;
        }

        for (final RecordField sourceField : sourceSchema.getFields()) {
            final Optional<RecordField> writeField = writeSchema.getField(sourceField.getFieldName());
            if (writeField.isEmpty() || !writeField.get().getFieldName().equals(sourceField.getFieldName())
                    || !isSerializedTypeCompatible(sourceField.getDataType(), writeField.get().getDataType())) {
                return null;
            }
        }

        final List<String> missingFields = new ArrayList<>(writeSchema.getFields().size() - sourceSchema.getFields().size());
        for (final RecordField writeField : writeSchema.getFields()) {
            if (sourceSchema.getField(writeField.getFieldName()).isEmpty()) {
                if (!writeField.isNullable() || writeField.getDefaultValue() != null) {
                    return null;
                }
                missingFields.add(writeField.getFieldName());
            }
        }

        return List.copyOf(missingFields);
    }

    private boolean isSerializedTypeCompatible(final DataType sourceType, final DataType writeType) {
        return sourceType.equals(writeType);
    }

    private boolean containsLineFeed(final Object serialized) {
        if (serialized instanceof final String serializedString) {
            return serializedString.indexOf('\n') >= 0 || serializedString.indexOf('\r') >= 0;
        }
        return ((Utf8JsonValue) serialized).containsLineBreak();
    }

    private boolean hasScientificNotation(final Object serialized) {
        if (serialized instanceof final String serializedString) {
            return SCIENTIFIC_NOTATION_PATTERN.matcher(serializedString).find();
        }

        return ((Utf8JsonValue) serialized).hasScientificNotation();
    }

    private record SerializedSchemaCompatibility(RecordSchema sourceSchema, boolean compatible, List<String> missingFields) {
        private static final SerializedSchemaCompatibility COMPATIBLE = new SerializedSchemaCompatibility(null, true, List.of());
        private static final SerializedSchemaCompatibility INCOMPATIBLE = new SerializedSchemaCompatibility(null, false, List.of());
    }

    private void writeRecord(final Record record, final RecordSchema writeSchema, final JsonGenerator generator,
        final GeneratorTask startTask, final GeneratorTask endTask, final boolean schemaAware) throws IOException {

        if (tryWriteSerializedRecord(record, writeSchema)) {
            return;
        }

        try {
            startTask.apply(generator);

            if (schemaAware) {
                for (final RecordField field : writeSchema.getFields()) {
                    final String fieldName = field.getFieldName();
                    final Object value = record.getValue(field);
                    if (value == null) {
                        if (nullSuppression == NullSuppression.NEVER_SUPPRESS || (nullSuppression == NullSuppression.SUPPRESS_MISSING) && isFieldPresent(field, record)) {
                            generator.writeNullField(fieldName);
                        }

                        continue;
                    }

                    generator.writeFieldName(fieldName);

                    writeValue(generator, value, fieldName, field.getDataType());
                }
            } else {
                for (final String fieldName : record.getRawFieldNames()) {
                    final Object value = record.getValue(fieldName);
                    if (value == null) {
                        if (nullSuppression != NullSuppression.ALWAYS_SUPPRESS) {
                            generator.writeNullField(fieldName);
                        }

                        continue;
                    }

                    generator.writeFieldName(fieldName);
                    writeRawValue(generator, value, fieldName);
                }
            }

            endTask.apply(generator);
        } catch (final Exception e) {
            logger.error("Failed to write {} with reader schema {} and writer schema {} as a JSON Object", record, record.getSchema(), writeSchema, e);
            throw e;
        }
    }

    private void writeSerializedObjectWithMissingFields(final Utf8JsonValue serialized, final List<String> missingFields) throws IOException {
        generator.writeStartObject();
        if (serialized.hasObjectMembers()) {
            generator.writeRaw(serialized.objectContents());
            generator.writeRaw(',');
        }
        for (final String fieldName : missingFields) {
            generator.writeNullField(fieldName);
        }
        generator.writeEndObject();
    }

    private boolean isFieldPresent(final RecordField field, final Record record) {
        final Set<String> rawFieldNames = record.getRawFieldNames();
        if (rawFieldNames.contains(field.getFieldName())) {
            return true;
        }

        for (final String alias : field.getAliases()) {
            if (rawFieldNames.contains(alias)) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    private void writeRawValue(final JsonGenerator generator, final Object value, final String fieldName) throws IOException {

        if (value == null) {
            generator.writeNull();
            return;
        }

        if (value instanceof final Record record) {
            writeRecord(record, record.getSchema(), generator, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, false);
            return;
        }

        if (value instanceof Map) {
            final Map<String, ?> map = (Map<String, ?>) value;
            generator.writeStartObject();

            for (final Map.Entry<String, ?> entry : map.entrySet()) {
                final String mapKey = entry.getKey();
                final Object mapValue = entry.getValue();
                generator.writeFieldName(mapKey);
                writeRawValue(generator, mapValue, fieldName + "." + mapKey);
            }

            generator.writeEndObject();
            return;
        }

        if (value instanceof final Object[] values) {
            generator.writeStartArray();
            for (final Object element : values) {
                writeRawValue(generator, element, fieldName);
            }
            generator.writeEndArray();
            return;
        }

        if (value instanceof Time) {
            final Object formatted = STRING_FIELD_CONVERTER.convertField(value, timeFormatOption, fieldName);
            generator.writeObject(formatted);
            return;
        }
        if (value instanceof java.sql.Date) {
            final Object formatted = STRING_FIELD_CONVERTER.convertField(value, dateFormatOption, fieldName);
            generator.writeObject(formatted);
            return;
        }
        if (value instanceof java.util.Date) {
            writeTimestamp(generator, value, fieldName);
            return;
        }
        if (!allowScientificNotation) {
            if (value instanceof Double || value instanceof Float) {
                generator.writeNumber(DataTypeUtils.toBigDecimal(value, fieldName));
                return;
            }
        }

        generator.writeObject(value);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final JsonGenerator generator, final Object value, final String fieldName, final DataType dataType) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }

        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        if (chosenDataType == null) {
            logger.debug("Could not find a suitable field type in the CHOICE for field {} and value {}; will use null value", fieldName, value);
            generator.writeNull();
            return;
        }

        final Object coercedValue = DataTypeUtils.convertType(
                value, chosenDataType, dateFormatOption, timeFormatOption, timestampFormatOption, fieldName
        );
        if (coercedValue == null) {
            generator.writeNull();
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = STRING_FIELD_CONVERTER.convertField(coercedValue, dateFormatOption, fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIME: {
                final String stringValue = STRING_FIELD_CONVERTER.convertField(coercedValue, timeFormatOption, fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
            }
            case TIMESTAMP: {
                writeTimestamp(generator, coercedValue, fieldName);
                break;
            }
            case DOUBLE:
                if (allowScientificNotation) {
                    generator.writeNumber(DataTypeUtils.toDouble(coercedValue, fieldName));
                } else {
                    generator.writeNumber(DataTypeUtils.toBigDecimal(coercedValue, fieldName));
                }
                break;
            case FLOAT:
                if (allowScientificNotation) {
                    generator.writeNumber(DataTypeUtils.toFloat(coercedValue, fieldName));
                } else {
                    generator.writeNumber(DataTypeUtils.toBigDecimal(coercedValue, fieldName));
                }
                break;
            case LONG:
                generator.writeNumber(DataTypeUtils.toLong(coercedValue, fieldName));
                break;
            case INT:
            case BYTE:
            case SHORT:
                generator.writeNumber(DataTypeUtils.toInteger(coercedValue, fieldName));
                break;
            case UUID:
            case CHAR:
            case STRING:
                generator.writeString(coercedValue.toString());
                break;
            case DECIMAL:
                generator.writeNumber(DataTypeUtils.toBigDecimal(coercedValue, fieldName));
                break;
            case BIGINT:
                if (coercedValue instanceof Long) {
                    generator.writeNumber((Long) coercedValue);
                } else {
                    generator.writeNumber((BigInteger) coercedValue);
                }
                break;
            case BOOLEAN:
                final String stringValue = coercedValue.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(true);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    generator.writeBoolean(false);
                } else {
                    generator.writeString(stringValue);
                }
                break;
            case RECORD: {
                final Record record = (Record) coercedValue;
                final RecordDataType recordDataType = (RecordDataType) chosenDataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();
                writeRecord(record, childSchema, generator, JsonGenerator::writeStartObject, JsonGenerator::writeEndObject, true);
                break;
            }
            case MAP: {
                final MapDataType mapDataType = (MapDataType) chosenDataType;
                final DataType valueDataType = mapDataType.getValueType();
                final Map<String, ?> map = (Map<String, ?>) coercedValue;
                generator.writeStartObject();

                for (final Map.Entry<String, ?> entry : map.entrySet()) {
                    final String mapKey = entry.getKey();
                    final Object mapValue = entry.getValue();
                    generator.writeFieldName(mapKey);
                    writeValue(generator, mapValue, fieldName + "." + mapKey, valueDataType);
                }
                generator.writeEndObject();
                break;
            }
            case ARRAY:
            default:
                if (coercedValue instanceof final Object[] values) {
                    final ArrayDataType arrayDataType = (ArrayDataType) chosenDataType;
                    final DataType elementType = arrayDataType.getElementType();
                    writeArray(values, fieldName, generator, elementType);
                } else {
                    generator.writeString(coercedValue.toString());
                }
                break;
        }
    }

    private void writeArray(final Object[] values, final String fieldName, final JsonGenerator generator, final DataType elementType) throws IOException {
        generator.writeStartArray();
        for (final Object element : values) {
            writeValue(generator, element, fieldName, elementType);
        }
        generator.writeEndArray();
    }

    private void writeTimestamp(final JsonGenerator generator, final Object value, final String fieldName) throws IOException {
        switch (timestampRepresentation) {
            case FORMATTED_STRING:
                generator.writeString(STRING_FIELD_CONVERTER.convertField(value, timestampFormatOption, fieldName));
                break;
            case EPOCH_MILLISECONDS:
                generator.writeNumber(DataTypeUtils.toLong(value, fieldName));
                break;
            case EPOCH_SECONDS:
                generator.writeNumber(BigDecimal.valueOf(DataTypeUtils.toLong(value, fieldName), 3));
                break;
            case AUTO:
            default:
                final String stringValue = STRING_FIELD_CONVERTER.convertField(value, timestampFormatOption, fieldName);
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    generator.writeNumber(DataTypeUtils.toLong(value, fieldName));
                } else {
                    generator.writeString(stringValue);
                }
                break;
        }
    }

    @Override
    public String getMimeType() {
        return this.mimeType;
    }

    private interface GeneratorTask {
        void apply(JsonGenerator generator) throws IOException;
    }
}
