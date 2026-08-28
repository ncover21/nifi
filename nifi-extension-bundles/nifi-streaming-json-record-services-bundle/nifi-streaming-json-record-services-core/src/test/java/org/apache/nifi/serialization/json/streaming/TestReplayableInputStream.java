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

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestReplayableInputStream {
    @Test
    void testReplaysCapturedPrefixThenContinuesSource() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        try (ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 64)) {
            input.mark(Integer.MAX_VALUE);
            assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), input.readNBytes(3));
            input.reset();
            input.completeSchemaAccess();

            assertArrayEquals(content, input.readAllBytes());
        }
    }

    @Test
    void testSpillsCaptureBeyondMemoryThresholdAndDeletesAfterReplay() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        try (ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4)) {
            input.mark(Integer.MAX_VALUE);
            assertArrayEquals("abcdef".getBytes(StandardCharsets.UTF_8), input.readNBytes(6));
            assertTrue(input.isFileBackedCapture());
            input.reset();

            assertArrayEquals(content, input.readAllBytes());
            assertFalse(input.isFileBackedCapture());
        }
    }

    @Test
    void testCloseDeletesSpilledCaptureBeforeReplayCompletes() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        final ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4);
        input.mark(Integer.MAX_VALUE);
        input.readNBytes(6);
        final Path capturePath = input.getCapturePath();

        input.close();

        assertFalse(Files.exists(capturePath));
        input.close();
    }

    @Test
    void testReadOperationsFailAfterClose() throws IOException {
        final ReplayableInputStream input = new ReplayableInputStream(
                new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        input.close();

        assertThrows(IOException.class, input::read);
        assertThrows(IOException.class, () -> input.read(new byte[1], 0, 1));
        assertThrows(IOException.class, input::available);
    }

    @Test
    void testRejectsCaptureBeyondTotalBound() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        try (ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4, 6)) {
            input.mark(Integer.MAX_VALUE);
            final IOException failure = assertThrows(IOException.class, () -> input.readNBytes(content.length));
            assertEquals("Schema inference replay exceeds the bounded capture limit of 6 bytes", failure.getMessage());
        }
    }

    @Test
    void testCacheHitDisablesCaptureWithoutReadingSource() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        try (ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4)) {
            input.completeSchemaAccess();

            assertArrayEquals(content, input.readAllBytes());
        }
    }

    @Test
    void testCaptureBufferAllocatedLazily() throws Exception {
        try (ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(new byte[0]), 8192)) {
            final Field field = ReplayableInputStream.class.getDeclaredField("memoryCapture");
            field.setAccessible(true);

            assertNull(field.get(input));
            input.completeSchemaAccess();
            assertNull(field.get(input));
        }
    }

    @Test
    void testCloseRetriesSourceCleanup() throws IOException {
        final FailOnceCloseInputStream source = new FailOnceCloseInputStream();
        final ReplayableInputStream input = new ReplayableInputStream(source);

        assertThrows(IOException.class, input::close);
        input.close();

        assertEquals(2, source.closeAttempts);
    }

    @Test
    void testRuntimeSourceCloseFailureDoesNotSkipCaptureCleanup() throws IOException {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        final RuntimeCloseInputStream source = new RuntimeCloseInputStream(content);
        final ReplayableInputStream input = new ReplayableInputStream(source, 4);
        input.mark(Integer.MAX_VALUE);
        input.readNBytes(6);
        final Path capturePath = input.getCapturePath();

        final IllegalStateException failure = assertThrows(IllegalStateException.class, input::close);

        assertEquals("source close failed", failure.getMessage());
        assertEquals(1, source.closeAttempts);
        assertFalse(Files.exists(capturePath));
    }

    @Test
    void testCloseRetriesSpilledCaptureCleanup() throws Exception {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        final ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4);
        input.mark(Integer.MAX_VALUE);
        input.readNBytes(6);
        final Field field = ReplayableInputStream.class.getDeclaredField("fileCapture");
        field.setAccessible(true);
        final FailOnceCloseChannel channel = new FailOnceCloseChannel((SeekableByteChannel) field.get(input));
        field.set(input, channel);

        assertThrows(IOException.class, input::close);
        input.close();

        assertEquals(2, channel.closeAttempts);
    }

    @Test
    void testCloseRetriesReplayCleanup() throws Exception {
        final byte[] content = "abcdefgh".getBytes(StandardCharsets.UTF_8);
        final ReplayableInputStream input = new ReplayableInputStream(new ByteArrayInputStream(content), 4);
        input.mark(Integer.MAX_VALUE);
        input.readNBytes(6);
        input.reset();
        final Field field = ReplayableInputStream.class.getDeclaredField("replay");
        field.setAccessible(true);
        final FailOnceCloseInputStream replay = new FailOnceCloseInputStream((InputStream) field.get(input));
        field.set(input, replay);

        assertThrows(IOException.class, input::close);
        input.close();

        assertEquals(2, replay.closeAttempts);
    }

    private static final class FailOnceCloseInputStream extends InputStream {
        private final InputStream delegate;
        private int closeAttempts;

        private FailOnceCloseInputStream() {
            this(InputStream.nullInputStream());
        }

        private FailOnceCloseInputStream(final InputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() throws IOException {
            closeAttempts++;
            if (closeAttempts == 1) {
                throw new IOException("close failed");
            }
            delegate.close();
        }
    }

    private static final class RuntimeCloseInputStream extends ByteArrayInputStream {
        private int closeAttempts;

        private RuntimeCloseInputStream(final byte[] content) {
            super(content);
        }

        @Override
        public void close() {
            closeAttempts++;
            throw new IllegalStateException("source close failed");
        }
    }

    private static final class FailOnceCloseChannel implements SeekableByteChannel {
        private final SeekableByteChannel delegate;
        private int closeAttempts;

        private FailOnceCloseChannel(final SeekableByteChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read(final ByteBuffer destination) throws IOException {
            return delegate.read(destination);
        }

        @Override
        public int write(final ByteBuffer source) throws IOException {
            return delegate.write(source);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public SeekableByteChannel position(final long newPosition) throws IOException {
            delegate.position(newPosition);
            return this;
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public SeekableByteChannel truncate(final long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            closeAttempts++;
            if (closeAttempts == 1) {
                throw new IOException("close failed");
            }
            delegate.close();
        }
    }
}
