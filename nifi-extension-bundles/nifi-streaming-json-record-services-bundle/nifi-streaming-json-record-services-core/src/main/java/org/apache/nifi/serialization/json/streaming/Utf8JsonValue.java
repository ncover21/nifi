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

import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.SerializedString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

final class Utf8JsonValue implements SerializableString {
    private static final int LINE_BREAK_KNOWN = 1;
    private static final int LINE_BREAK_PRESENT = 1 << 1;
    private static final int SCIENTIFIC_NOTATION_KNOWN = 1 << 2;
    private static final int SCIENTIFIC_NOTATION_PRESENT = 1 << 3;
    private static final int OBJECT_MEMBERS_KNOWN = 1 << 4;
    private static final int OBJECT_MEMBERS_PRESENT = 1 << 5;

    private final byte[] source;
    private final int offset;
    private final int length;
    private final int metadata;
    private volatile SerializedString delegate;

    Utf8JsonValue(final byte[] source) {
        this(source, 0, source.length);
    }

    Utf8JsonValue(final byte[] source, final int offset, final int length) {
        this(source, offset, length, 0);
    }

    Utf8JsonValue(final byte[] source, final int offset, final int length, final boolean containsLineBreak) {
        this(source, offset, length, known(LINE_BREAK_KNOWN, LINE_BREAK_PRESENT, containsLineBreak));
    }

    Utf8JsonValue(final byte[] source, final int offset, final int length, final boolean containsLineBreak, final boolean hasObjectMembers) {
        this(source, offset, length, known(LINE_BREAK_KNOWN, LINE_BREAK_PRESENT, containsLineBreak)
                | known(OBJECT_MEMBERS_KNOWN, OBJECT_MEMBERS_PRESENT, hasObjectMembers));
    }

    Utf8JsonValue(final byte[] source, final int offset, final int length, final boolean containsLineBreak,
                  final boolean containsScientificNotation, final boolean hasObjectMembers) {
        this(source, offset, length, known(LINE_BREAK_KNOWN, LINE_BREAK_PRESENT, containsLineBreak)
                | known(SCIENTIFIC_NOTATION_KNOWN, SCIENTIFIC_NOTATION_PRESENT, containsScientificNotation)
                | known(OBJECT_MEMBERS_KNOWN, OBJECT_MEMBERS_PRESENT, hasObjectMembers));
    }

    private Utf8JsonValue(final byte[] source, final int offset, final int length, final int metadata) {
        this.source = Objects.requireNonNull(source, "Source required");
        Objects.checkFromIndexSize(offset, length, source.length);
        this.offset = offset;
        this.length = length;
        this.metadata = metadata;
    }

    static boolean isUtf8EncodedJson(final byte[] source) {
        return isUtf8EncodedJson(source, 0, source.length);
    }

    static boolean isUtf8EncodedJson(final byte[] source, final int offset, final int length) {
        Objects.checkFromIndexSize(offset, length, source.length);
        if (length >= 4) {
            final int first = source[offset] & 0xFF;
            final int second = source[offset + 1] & 0xFF;
            final int third = source[offset + 2] & 0xFF;
            final int fourth = source[offset + 3] & 0xFF;
            if ((first == 0 && second == 0 && third == 0xFE && fourth == 0xFF)
                    || (first == 0xFF && second == 0xFE && third == 0 && fourth == 0)) {
                return false;
            }
        }
        if (length >= 3 && source[offset] == (byte) 0xEF && source[offset + 1] == (byte) 0xBB
                && source[offset + 2] == (byte) 0xBF) {
            return false;
        }
        if (length >= 2) {
            final int first = source[offset] & 0xFF;
            final int second = source[offset + 1] & 0xFF;
            if ((first == 0xFE && second == 0xFF) || (first == 0xFF && second == 0xFE)) {
                return false;
            }
            if (length < 4 && (first == 0 || second == 0)) {
                return false;
            }
        }
        if (length >= 4) {
            final int first = source[offset] & 0xFF;
            final int second = source[offset + 1] & 0xFF;
            final int third = source[offset + 2] & 0xFF;
            final int fourth = source[offset + 3] & 0xFF;
            return !((first == 0 && second == 0 && third == 0)
                    || (second == 0 && third == 0 && fourth == 0)
                    || (first == 0 && third == 0)
                    || (second == 0 && fourth == 0));
        }
        return true;
    }

    boolean containsLineBreak() {
        if ((metadata & LINE_BREAK_KNOWN) != 0) {
            return (metadata & LINE_BREAK_PRESENT) != 0;
        }
        for (int i = offset; i < offset + length; i++) {
            if (source[i] == '\n' || source[i] == '\r') {
                return true;
            }
        }
        return false;
    }

    boolean hasScientificNotation() {
        if ((metadata & SCIENTIFIC_NOTATION_KNOWN) != 0) {
            return (metadata & SCIENTIFIC_NOTATION_PRESENT) != 0;
        }
        final int end = offset + length;
        for (int i = offset + 1; i < end - 1; i++) {
            if ((source[i] == 'e' || source[i] == 'E') && isDigit(source[i - 1])) {
                final int exponentIndex = source[i + 1] == '+' || source[i + 1] == '-' ? i + 2 : i + 1;
                if (exponentIndex < end && isDigit(source[exponentIndex])) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean isBackedBy(final byte[] bytes) {
        return source == bytes;
    }

    boolean hasObjectMembers() {
        if ((metadata & OBJECT_MEMBERS_KNOWN) != 0) {
            return (metadata & OBJECT_MEMBERS_PRESENT) != 0;
        }
        for (int i = offset + 1; i < offset + length - 1; i++) {
            if (!isJsonWhitespace(source[i])) {
                return true;
            }
        }
        return false;
    }

    Utf8JsonValue objectContents() {
        if (length < 2 || source[offset] != '{' || source[offset + length - 1] != '}') {
            throw new IllegalStateException("Serialized JSON record must be an object");
        }
        final int objectMetadata = metadata & (LINE_BREAK_KNOWN | LINE_BREAK_PRESENT
                | SCIENTIFIC_NOTATION_KNOWN | SCIENTIFIC_NOTATION_PRESENT);
        return new Utf8JsonValue(source, offset + 1, length - 2, objectMetadata);
    }

    @Override
    public String getValue() {
        return getDelegate().getValue();
    }

    @Override
    public int charLength() {
        return getDelegate().charLength();
    }

    @Override
    public char[] asQuotedChars() {
        return getDelegate().asQuotedChars();
    }

    @Override
    public byte[] asUnquotedUTF8() {
        return Arrays.copyOfRange(source, offset, offset + length);
    }

    @Override
    public byte[] asQuotedUTF8() {
        return getDelegate().asQuotedUTF8();
    }

    @Override
    public int appendQuotedUTF8(final byte[] buffer, final int bufferOffset) {
        return getDelegate().appendQuotedUTF8(buffer, bufferOffset);
    }

    @Override
    public int appendQuoted(final char[] buffer, final int bufferOffset) {
        return getDelegate().appendQuoted(buffer, bufferOffset);
    }

    @Override
    public int appendUnquotedUTF8(final byte[] buffer, final int bufferOffset) {
        if (bufferOffset < 0 || bufferOffset > buffer.length || length > buffer.length - bufferOffset) {
            return -1;
        }
        System.arraycopy(source, offset, buffer, bufferOffset, length);
        return length;
    }

    @Override
    public int appendUnquoted(final char[] buffer, final int bufferOffset) {
        return getDelegate().appendUnquoted(buffer, bufferOffset);
    }

    @Override
    public int writeQuotedUTF8(final OutputStream output) throws IOException {
        return getDelegate().writeQuotedUTF8(output);
    }

    @Override
    public int writeUnquotedUTF8(final OutputStream output) throws IOException {
        output.write(source, offset, length);
        return length;
    }

    @Override
    public int putQuotedUTF8(final ByteBuffer buffer) throws IOException {
        return getDelegate().putQuotedUTF8(buffer);
    }

    @Override
    public int putUnquotedUTF8(final ByteBuffer buffer) {
        if (buffer.remaining() < length) {
            return -1;
        }
        buffer.put(source, offset, length);
        return length;
    }

    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof final Utf8JsonValue that) || length != that.length) {
            return false;
        }
        return Arrays.equals(source, offset, offset + length, that.source, that.offset, that.offset + that.length);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = offset; i < offset + length; i++) {
            hash = 31 * hash + source[i];
        }
        return hash;
    }

    private boolean isDigit(final byte value) {
        return value >= '0' && value <= '9';
    }

    private boolean isJsonWhitespace(final byte value) {
        return value == ' ' || value == '\t' || value == '\n' || value == '\r';
    }

    private static int known(final int knownFlag, final int valueFlag, final boolean value) {
        return knownFlag | (value ? valueFlag : 0);
    }

    private SerializedString getDelegate() {
        SerializedString current = delegate;
        if (current == null) {
            current = new SerializedString(new String(source, offset, length, StandardCharsets.UTF_8));
            delegate = current;
        }
        return current;
    }
}
