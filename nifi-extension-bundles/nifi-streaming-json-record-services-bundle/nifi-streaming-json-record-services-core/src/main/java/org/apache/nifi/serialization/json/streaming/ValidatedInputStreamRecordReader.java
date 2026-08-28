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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.json.streaming.StreamingJsonSchemaInference.JsonRecordMetadata;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

final class ValidatedInputStreamRecordReader implements RecordReader {
    static final int MAX_DEFERRED_RECORD_BYTES = RecordCapturingInputStream.MAX_RECORD_CAPTURE_BYTES;
    static final long MAX_DEFERRED_TOTAL_BYTES = 64L * 1024 * 1024;
    private static final int SMALL_DISCARD_THRESHOLD = 64;
    private static final int SKIP_BUFFER_SIZE = 8192;

    private final InputStream input;
    private final RecordSchema schema;
    private final List<JsonRecordMetadata> records;
    private final ValidatedJsonRecordFactory recordFactory;

    private byte[] skipBuffer;
    private long position;
    private int recordIndex;
    private boolean failed;
    private boolean closeRequested;
    private boolean cleanupComplete;

    ValidatedInputStreamRecordReader(final InputStream input, final ComponentLog logger, final RecordSchema schema,
                                     final List<JsonRecordMetadata> records, final String dateFormat, final String timeFormat,
                                     final String timestampFormat, final TokenParserFactory tokenParserFactory) {
        if (!isMetadataSupported(records)) {
            throw new IllegalArgumentException("Validated JSON record metadata contains unsupported byte offsets");
        }
        this.input = input;
        this.schema = schema;
        this.records = records;
        this.recordFactory = new ValidatedJsonRecordFactory(logger, schema, dateFormat, timeFormat, timestampFormat, tokenParserFactory);
    }

    static boolean isMetadataSupported(final List<JsonRecordMetadata> records) {
        long previousEnd = 0;
        long totalBytes = 0;
        for (final JsonRecordMetadata metadata : records) {
            final long start = metadata.startOffset();
            final long end = metadata.endOffset();
            final long recordBytes = end - start;
            if (start < 0 || start < previousEnd || end < start || recordBytes > MAX_DEFERRED_RECORD_BYTES
                    || totalBytes > MAX_DEFERRED_TOTAL_BYTES - recordBytes) {
                return false;
            }
            totalBytes += recordBytes;
            previousEnd = end;
        }
        return true;
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        ensureOpen();
        if (failed) {
            throw new IOException("Record Reader cannot continue after a validated JSON second-pass failure");
        }
        if (recordIndex == records.size()) {
            return null;
        }

        final JsonRecordMetadata metadata = records.get(recordIndex);
        try {
            discard(metadata.startOffset() - position);
            final int length = Math.toIntExact(metadata.endOffset() - metadata.startOffset());
            final byte[] recordBytes = new byte[length];
            readFully(recordBytes);
            position = metadata.endOffset();
            recordIndex++;
            return recordFactory.createRecord(recordBytes, 0, length, metadata, coerceTypes, dropUnknownFields);
        } catch (final IOException | RuntimeException | Error e) {
            failed = true;
            throw e;
        }
    }

    private void discard(final long length) throws IOException {
        if (length == 0) {
            return;
        }
        long remaining = length;
        if (remaining <= SMALL_DISCARD_THRESHOLD) {
            while (remaining > 0) {
                if (input.read() < 0) {
                    throw new EOFException("Unexpected end of JSON before validated record offset");
                }
                remaining--;
            }
            position += length;
            return;
        }

        if (skipBuffer == null) {
            skipBuffer = new byte[SKIP_BUFFER_SIZE];
        }
        while (remaining > 0) {
            final int requested = (int) Math.min(remaining, skipBuffer.length);
            final int count = input.read(skipBuffer, 0, requested);
            if (count < 0) {
                throw new EOFException("Unexpected end of JSON before validated record offset");
            }
            if (count == 0) {
                if (input.read() < 0) {
                    throw new EOFException("Unexpected end of JSON before validated record offset");
                }
                remaining--;
            } else {
                remaining -= count;
            }
        }
        position += length;
    }

    private void readFully(final byte[] recordBytes) throws IOException {
        int offset = 0;
        while (offset < recordBytes.length) {
            final int count = input.read(recordBytes, offset, recordBytes.length - offset);
            if (count < 0) {
                throw new EOFException("Unexpected end of JSON within validated record range");
            }
            if (count == 0) {
                final int value = input.read();
                if (value < 0) {
                    throw new EOFException("Unexpected end of JSON within validated record range");
                }
                recordBytes[offset++] = (byte) value;
            } else {
                offset += count;
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closeRequested) {
            throw new IOException("Record Reader is closed");
        }
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public void close() throws IOException {
        closeRequested = true;
        skipBuffer = null;
        if (!cleanupComplete) {
            input.close();
            cleanupComplete = true;
        }
    }
}
