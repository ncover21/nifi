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

import com.fasterxml.jackson.core.JsonParser;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.json.streaming.StreamingJsonSchemaInference.JsonRecordMetadata;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

final class ValidatedJsonRecordFactory implements DeferredJsonRecord.RecordMaterializer {
    private static final String MIME_TYPE = "application/json";

    private final ComponentLog logger;
    private final RecordSchema schema;
    private final String dateFormat;
    private final String timeFormat;
    private final String timestampFormat;
    private final TokenParserFactory tokenParserFactory;
    private final SchemaMutationSnapshot schemaMutationSnapshot;

    ValidatedJsonRecordFactory(final ComponentLog logger, final RecordSchema schema, final String dateFormat,
                               final String timeFormat, final String timestampFormat,
                               final TokenParserFactory tokenParserFactory) {
        this.logger = logger;
        this.schema = schema;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
        this.timestampFormat = timestampFormat;
        this.tokenParserFactory = tokenParserFactory;
        this.schemaMutationSnapshot = SchemaMutationSnapshot.capture(schema);
    }

    Record createRecord(final byte[] source, final int offset, final int length, final JsonRecordMetadata metadata,
                        final boolean coerceTypes, final boolean dropUnknownFields) throws MalformedRecordException {
        if (offset < 0 || length < 0 || offset > source.length - length) {
            throw new MalformedRecordException("Invalid validated JSON record byte offsets");
        }

        final SerializedForm serializedForm = metadata.containsDuplicateFields()
                ? SerializedForm.of((Supplier<String>) () -> normalizeJson(source, offset, length), MIME_TYPE)
                : SerializedForm.of(new Utf8JsonValue(source, offset, length, metadata.containsLineBreak(),
                        metadata.containsScientificNotation(), metadata.hasObjectMembers()), MIME_TYPE);
        return new DeferredJsonRecord(schema, coerceTypes, dropUnknownFields, serializedForm, this, source, offset, length, schemaMutationSnapshot);
    }

    private String normalizeJson(final byte[] source, final int offset, final int length) {
        try (final JsonParser parser = tokenParserFactory.getJsonParser(source, offset, length)) {
            return parser.readValueAsTree().toString();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Record materialize(final byte[] source, final int offset, final int length,
                              final boolean coerceTypes, final boolean dropUnknownFields)
            throws IOException, MalformedRecordException {
        try (final StreamingJsonRowRecordReader reader = new StreamingJsonRowRecordReader(source, offset, length, logger, schema,
                dateFormat, timeFormat, timestampFormat, StartingFieldStrategy.ROOT_NODE, null,
                SchemaApplicationStrategy.SELECTED_PART, tokenParserFactory)) {
            final Record record = reader.nextRecord(coerceTypes, dropUnknownFields);
            if (record == null) {
                throw new MalformedRecordException("Validated JSON record did not contain an object");
            }
            return record;
        }
    }
}
