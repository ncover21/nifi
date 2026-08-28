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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

final class ReplayableInputStream extends InputStream {
    static final int DEFAULT_MEMORY_THRESHOLD_BYTES = 1_048_576;
    static final long DEFAULT_MAX_CAPTURE_BYTES = 1_073_741_824L;

    private final InputStream source;
    private final int memoryThresholdBytes;
    private final long maxCaptureBytes;
    private byte[] memoryCapture;
    private int memoryCaptureLength;
    private ByteBuffer singleByteBuffer;
    private SeekableByteChannel fileCapture;
    private Path capturePath;
    private InputStream replay;
    private long capturedBytes;
    private boolean capturing = true;
    private boolean marked;
    private boolean closed;
    private boolean sourceClosed;

    ReplayableInputStream(final InputStream source) {
        this(source, DEFAULT_MEMORY_THRESHOLD_BYTES);
    }

    ReplayableInputStream(final InputStream source, final int memoryThresholdBytes) {
        this(source, memoryThresholdBytes, DEFAULT_MAX_CAPTURE_BYTES);
    }

    ReplayableInputStream(final InputStream source, final int memoryThresholdBytes, final long maxCaptureBytes) {
        if (memoryThresholdBytes < 0) {
            throw new IllegalArgumentException("Memory threshold cannot be negative");
        }
        if (maxCaptureBytes < memoryThresholdBytes) {
            throw new IllegalArgumentException("Maximum capture size cannot be less than the memory threshold");
        }
        this.source = source;
        this.memoryThresholdBytes = memoryThresholdBytes;
        this.maxCaptureBytes = maxCaptureBytes;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        final int replayed = readReplayByte();
        if (replayed >= 0) {
            return replayed;
        }

        final int value = source.read();
        if (value >= 0 && capturing) {
            writeCapture(value);
        }
        return value;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        ensureOpen();
        if (length == 0) {
            return 0;
        }

        final int replayed = readReplay(buffer, offset, length);
        if (replayed >= 0) {
            return replayed;
        }

        final int count = source.read(buffer, offset, length);
        if (count > 0 && capturing) {
            writeCapture(buffer, offset, count);
        }
        return count;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        final long available = (replay == null ? 0L : replay.available()) + (long) source.available();
        return (int) Math.min(Integer.MAX_VALUE, available);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(final int readLimit) {
        marked = capturing;
    }

    @Override
    public synchronized void reset() throws IOException {
        ensureOpen();
        if (!marked || !capturing) {
            throw new IOException("Stream has not been marked for replay");
        }

        capturing = false;
        marked = false;
        if (capturePath == null) {
            if (memoryCaptureLength > 0) {
                replay = new ByteArrayInputStream(memoryCapture, 0, memoryCaptureLength);
            }
            memoryCapture = null;
        } else {
            fileCapture.position(0);
            replay = Channels.newInputStream(fileCapture);
            fileCapture = null;
        }
    }

    void completeSchemaAccess() throws IOException {
        ensureOpen();
        if (capturing) {
            capturing = false;
            marked = false;
            memoryCapture = null;
            Throwable failure = closeFileCapture(null);
            failure = deleteCapture(failure);
            if (failure != null) {
                throwFailure(failure);
            }
        }
    }

    boolean isFileBackedCapture() {
        return capturePath != null;
    }

    Path getCapturePath() {
        return capturePath;
    }

    @Override
    public void close() throws IOException {
        closed = true;

        Throwable failure = closeReplay(null);
        failure = closeFileCapture(failure);
        failure = closeSource(failure);
        memoryCapture = null;
        failure = deleteCapture(failure);
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private int readReplayByte() throws IOException {
        if (replay == null) {
            return -1;
        }
        final int value = replay.read();
        if (value < 0) {
            finishReplay();
        }
        return value;
    }

    private int readReplay(final byte[] buffer, final int offset, final int length) throws IOException {
        if (replay == null) {
            return -1;
        }
        final int count = replay.read(buffer, offset, length);
        if (count < 0) {
            finishReplay();
        }
        return count;
    }

    private void finishReplay() throws IOException {
        Throwable failure = closeReplay(null);
        failure = deleteCapture(failure);
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private void writeCapture(final int value) throws IOException {
        ensureCaptureLimit(1);
        if (capturePath == null && memoryCaptureLength < memoryThresholdBytes) {
            ensureMemoryCapacity(memoryCaptureLength + 1);
            memoryCapture[memoryCaptureLength++] = (byte) value;
            capturedBytes++;
            return;
        }
        ensureFileCapture();
        if (singleByteBuffer == null) {
            singleByteBuffer = ByteBuffer.allocate(1);
        }
        singleByteBuffer.clear();
        singleByteBuffer.put((byte) value);
        singleByteBuffer.flip();
        writeFully(fileCapture, singleByteBuffer);
        capturedBytes++;
    }

    private void writeCapture(final byte[] buffer, final int offset, final int length) throws IOException {
        ensureCaptureLimit(length);
        if (capturePath == null && memoryCaptureLength + (long) length <= memoryThresholdBytes) {
            ensureMemoryCapacity(memoryCaptureLength + length);
            System.arraycopy(buffer, offset, memoryCapture, memoryCaptureLength, length);
            memoryCaptureLength += length;
            capturedBytes += length;
            return;
        }
        ensureFileCapture();
        writeFully(fileCapture, ByteBuffer.wrap(buffer, offset, length));
        capturedBytes += length;
    }

    private void ensureFileCapture() throws IOException {
        if (capturePath != null) {
            return;
        }

        Path newPath = null;
        SeekableByteChannel newCapture = null;
        try {
            newPath = Files.createTempFile("nifi-streaming-json-replay-", ".tmp");
            newCapture = Files.newByteChannel(newPath, EnumSet.of(
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE));
            if (memoryCaptureLength > 0) {
                writeFully(newCapture, ByteBuffer.wrap(memoryCapture, 0, memoryCaptureLength));
            }
            memoryCapture = null;
            capturePath = newPath;
            fileCapture = newCapture;
        } catch (final IOException | RuntimeException | Error e) {
            final Throwable closeFailure = closeResource(newCapture, null);
            if (closeFailure != null && closeFailure != e) {
                e.addSuppressed(closeFailure);
            }
            if (newPath != null) {
                try {
                    Files.deleteIfExists(newPath);
                } catch (final IOException deleteFailure) {
                    if (deleteFailure != e) {
                        e.addSuppressed(deleteFailure);
                    }
                }
            }
            throw e;
        }
    }

    private void ensureMemoryCapacity(final int requiredCapacity) {
        if (memoryCapture == null) {
            memoryCapture = new byte[Math.min(memoryThresholdBytes, Math.max(requiredCapacity, 8192))];
            return;
        }
        if (memoryCapture.length >= requiredCapacity) {
            return;
        }
        final int doubledCapacity = Math.max(1, memoryCapture.length << 1);
        final int newCapacity = Math.min(memoryThresholdBytes, Math.max(requiredCapacity, doubledCapacity));
        final byte[] expanded = new byte[newCapacity];
        System.arraycopy(memoryCapture, 0, expanded, 0, memoryCaptureLength);
        memoryCapture = expanded;
    }

    private void ensureCaptureLimit(final int additionalBytes) throws IOException {
        if (capturedBytes + additionalBytes > maxCaptureBytes) {
            throw new IOException("Schema inference replay exceeds the bounded capture limit of " + maxCaptureBytes + " bytes");
        }
    }

    private void writeFully(final SeekableByteChannel channel, final ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private Throwable deleteCapture(final Throwable currentFailure) {
        if (capturePath == null) {
            return currentFailure;
        }
        try {
            Files.deleteIfExists(capturePath);
            capturePath = null;
            return currentFailure;
        } catch (final IOException | RuntimeException | Error deleteFailure) {
            return addFailure(currentFailure, deleteFailure);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private Throwable closeResource(final AutoCloseable resource, final Throwable currentFailure) {
        if (resource == null) {
            return currentFailure;
        }
        try {
            resource.close();
            return currentFailure;
        } catch (final Exception | Error e) {
            final Throwable closeFailure = e instanceof IOException || e instanceof RuntimeException || e instanceof Error
                    ? e : new IOException("Failed to close replay resource", e);
            return addFailure(currentFailure, closeFailure);
        }
    }

    private Throwable closeSource(final Throwable currentFailure) {
        if (sourceClosed) {
            return currentFailure;
        }
        try {
            source.close();
            sourceClosed = true;
            return currentFailure;
        } catch (final IOException | RuntimeException | Error e) {
            return addFailure(currentFailure, e);
        }
    }

    private Throwable closeReplay(final Throwable currentFailure) {
        if (replay == null) {
            return currentFailure;
        }
        try {
            replay.close();
            replay = null;
            return currentFailure;
        } catch (final IOException | RuntimeException | Error e) {
            return addFailure(currentFailure, e);
        }
    }

    private Throwable closeFileCapture(final Throwable currentFailure) {
        if (fileCapture == null) {
            return currentFailure;
        }
        try {
            fileCapture.close();
            fileCapture = null;
            return currentFailure;
        } catch (final IOException | RuntimeException | Error e) {
            return addFailure(currentFailure, e);
        }
    }

    private Throwable addFailure(final Throwable currentFailure, final Throwable additionalFailure) {
        if (currentFailure == null) {
            return additionalFailure;
        }
        if (currentFailure != additionalFailure) {
            currentFailure.addSuppressed(additionalFailure);
        }
        return currentFailure;
    }

    private void throwFailure(final Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw (Error) failure;
    }
}
