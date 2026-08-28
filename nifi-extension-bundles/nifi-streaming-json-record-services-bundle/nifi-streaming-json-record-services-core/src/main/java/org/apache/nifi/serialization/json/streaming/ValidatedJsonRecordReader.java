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

import org.apache.nifi.serialization.json.streaming.StreamingJsonSchemaInference.JsonRecordMetadata;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;

final class ValidatedJsonRecordReader implements RecordReader {
    private final byte[] input;
    private final RecordSchema schema;
    private final List<JsonRecordMetadata> records;
    private final ValidatedJsonRecordFactory recordFactory;

    private int recordIndex;

    ValidatedJsonRecordReader(final byte[] input, final ComponentLog logger, final RecordSchema schema,
                              final List<JsonRecordMetadata> records, final String dateFormat, final String timeFormat,
                              final String timestampFormat, final TokenParserFactory tokenParserFactory) {
        this.input = input;
        this.schema = schema;
        this.records = records;
        this.recordFactory = new ValidatedJsonRecordFactory(logger, schema, dateFormat, timeFormat, timestampFormat, tokenParserFactory);
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws MalformedRecordException {
        if (recordIndex == records.size()) {
            return null;
        }

        final JsonRecordMetadata metadata = records.get(recordIndex++);
        final int offset;
        final int length;
        try {
            offset = Math.toIntExact(metadata.startOffset());
            length = Math.toIntExact(metadata.endOffset() - metadata.startOffset());
        } catch (final ArithmeticException e) {
            throw new MalformedRecordException("Invalid validated JSON record byte offsets", e);
        }
        return recordFactory.createRecord(input, offset, length, metadata, coerceTypes, dropUnknownFields);
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() {
    }
}
