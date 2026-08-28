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
import java.util.Objects;

final class JsonEncodingProbeInputStream extends InputStream {
    private static final int PREFIX_LENGTH = 4;

    private final InputStream delegate;
    private final byte[] prefix = new byte[PREFIX_LENGTH];
    private int prefixLength;

    JsonEncodingProbeInputStream(final InputStream delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Input Stream required");
    }

    @Override
    public int read() throws IOException {
        final int value = delegate.read();
        if (value >= 0 && prefixLength < prefix.length) {
            prefix[prefixLength++] = (byte) value;
        }
        return value;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        final int count = delegate.read(buffer, offset, length);
        if (count > 0 && prefixLength < prefix.length) {
            final int copied = Math.min(count, prefix.length - prefixLength);
            System.arraycopy(buffer, offset, prefix, prefixLength, copied);
            prefixLength += copied;
        }
        return count;
    }

    boolean isUtf8EncodedJson() {
        return Utf8JsonValue.isUtf8EncodedJson(prefix, 0, prefixLength);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
