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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

final class RecordCapturingInputStream extends InputStream {
    static final int MAX_RECORD_CAPTURE_BYTES = 16 * 1024 * 1024;
    private static final int MINIMUM_CAPACITY = 256;
    private static final int MAXIMUM_REUSABLE_CAPACITY = 65_536;
    private static final int PASSIVE_LOOKBEHIND = 8;

    private final InputStream delegate;
    private byte[] buffer = new byte[0];
    private int start;
    private int end;
    private long absoluteStart;
    private long absolutePosition;
    private boolean recordActive;
    private boolean recordCaptureExceeded;

    RecordCapturingInputStream(final InputStream delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Input Stream required");
    }

    @Override
    public int read() throws IOException {
        final int value = delegate.read();
        if (value >= 0) {
            if (recordActive) {
                if (recordCaptureExceeded) {
                    appendPassive((byte) value);
                } else if (end - start == MAX_RECORD_CAPTURE_BYTES) {
                    recordCaptureExceeded = true;
                    appendOverflow((byte) value);
                } else {
                    ensureAppendCapacity(1);
                    buffer[end++] = (byte) value;
                }
            } else {
                appendPassive((byte) value);
            }
            absolutePosition++;
        }
        return value;
    }

    @Override
    public int read(final byte[] destination, final int offset, final int length) throws IOException {
        final int count = delegate.read(destination, offset, length);
        if (count > 0) {
            if (recordActive) {
                if (recordCaptureExceeded) {
                    appendPassive(destination, offset, count);
                } else if (count > MAX_RECORD_CAPTURE_BYTES - (end - start)) {
                    recordCaptureExceeded = true;
                    appendOverflow(destination, offset, count);
                } else {
                    ensureAppendCapacity(count);
                    System.arraycopy(destination, offset, buffer, end, count);
                    end += count;
                }
            } else {
                appendPassive(destination, offset, count);
            }
            absolutePosition += count;
        }
        return count;
    }

    boolean startRecord(final long position) {
        if (recordActive) {
            throw new IllegalStateException("JSON record capture is already active");
        }
        if (!contains(position, position)) {
            return false;
        }

        final int discarded = Math.toIntExact(position - absoluteStart);
        start += discarded;
        absoluteStart = position;
        recordActive = true;
        recordCaptureExceeded = false;
        return true;
    }

    boolean contains(final long from, final long to) {
        return from >= absoluteStart && to >= from && to <= absoluteStart + end - start;
    }

    byte[] copyRange(final long from, final long to) {
        if (!contains(from, to)) {
            throw new IllegalArgumentException("Requested range is not retained");
        }
        final int relativeStart = start + Math.toIntExact(from - absoluteStart);
        final int relativeEnd = start + Math.toIntExact(to - absoluteStart);
        return Arrays.copyOfRange(buffer, relativeStart, relativeEnd);
    }

    void finishRecord(final long position) {
        if (!recordActive) {
            throw new IllegalArgumentException("JSON record end is not retained");
        }
        if (!contains(position, position)) {
            if (!recordCaptureExceeded) {
                throw new IllegalArgumentException("JSON record end is not retained");
            }
            start = 0;
            end = 0;
            absoluteStart = absolutePosition;
            recordActive = false;
            recordCaptureExceeded = false;
            shrinkAfterRecord();
            return;
        }

        final int discarded = Math.toIntExact(position - absoluteStart);
        start += discarded;
        absoluteStart = position;
        recordActive = false;
        recordCaptureExceeded = false;
        if (start == end) {
            start = 0;
            end = 0;
        }
        shrinkAfterRecord();
    }

    int getRetainedByteCount() {
        return end - start;
    }

    int getBufferCapacity() {
        return buffer.length;
    }

    boolean isRecordCaptureExceeded() {
        return recordCaptureExceeded;
    }

    @Override
    public void close() throws IOException {
        buffer = new byte[0];
        start = 0;
        end = 0;
        delegate.close();
    }

    private void appendPassive(final byte value) {
        final int preceding = retainPassiveLookbehind(1);
        ensurePassiveCapacity(preceding + 1);
        buffer[preceding] = value;
        start = 0;
        end = preceding + 1;
        absoluteStart = absolutePosition - preceding;
    }

    private void appendPassive(final byte[] source, final int offset, final int length) {
        final int retained = Math.min(length, MAXIMUM_REUSABLE_CAPACITY);
        final int preceding = length == retained ? retainPassiveLookbehind(retained) : 0;
        ensurePassiveCapacity(preceding + retained);
        System.arraycopy(source, offset + length - retained, buffer, preceding, retained);
        start = 0;
        end = preceding + retained;
        absoluteStart = length == retained ? absolutePosition - preceding : absolutePosition + length - retained;
    }

    private void appendOverflow(final byte value) {
        buffer = new byte[Math.max(MINIMUM_CAPACITY, 1)];
        buffer[0] = value;
        start = 0;
        end = 1;
        absoluteStart = absolutePosition;
    }

    private void appendOverflow(final byte[] source, final int offset, final int length) {
        final int retained = Math.min(length, MAXIMUM_REUSABLE_CAPACITY);
        buffer = new byte[Math.max(MINIMUM_CAPACITY, retained)];
        System.arraycopy(source, offset + length - retained, buffer, 0, retained);
        start = 0;
        end = retained;
        absoluteStart = absolutePosition + length - retained;
    }

    private int retainPassiveLookbehind(final int currentLength) {
        final int preceding = Math.min(end - start,
                Math.min(PASSIVE_LOOKBEHIND, MAXIMUM_REUSABLE_CAPACITY - currentLength));
        System.arraycopy(buffer, end - preceding, buffer, 0, preceding);
        return preceding;
    }

    private void ensurePassiveCapacity(final int required) {
        if (buffer.length < required) {
            buffer = new byte[Math.max(MINIMUM_CAPACITY, required)];
        }
    }

    private void ensureAppendCapacity(final int additional) throws IOException {
        final int retained = end - start;
        if (additional > Integer.MAX_VALUE - retained) {
            throw new IOException("JSON record is too large to capture");
        }
        if (additional <= buffer.length - end) {
            return;
        }

        final int required = retained + additional;
        if (required <= buffer.length) {
            System.arraycopy(buffer, start, buffer, 0, retained);
            start = 0;
            end = retained;
            return;
        }

        final int doubled = buffer.length > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : buffer.length * 2;
        final byte[] expanded = new byte[Math.max(required, Math.max(MINIMUM_CAPACITY, doubled))];
        System.arraycopy(buffer, start, expanded, 0, retained);
        buffer = expanded;
        start = 0;
        end = retained;
    }

    private void shrinkAfterRecord() {
        final int retained = end - start;
        final int reusableCapacity = Math.max(MAXIMUM_REUSABLE_CAPACITY, Math.max(MINIMUM_CAPACITY, retained));
        if (buffer.length <= reusableCapacity) {
            return;
        }

        final byte[] reduced = new byte[reusableCapacity];
        System.arraycopy(buffer, start, reduced, 0, retained);
        buffer = reduced;
        start = 0;
        end = retained;
    }
}
