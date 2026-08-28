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

import org.apache.nifi.controller.repository.io.TaskTerminationInputStream;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestInferSchemaAccessStrategy {
    @Test
    void testInferenceFailureIsNotMaskedByResetFailure() {
        final IOException inferenceFailure = new IOException("inference failed");
        final IOException resetFailure = new IOException("reset failed");
        final RecordSourceFactory<Object> sourceFactory = (variables, input) -> () -> null;
        final SchemaInferenceEngine<Object> inference = source -> {
            throw inferenceFailure;
        };
        final InferSchemaAccessStrategy<Object> strategy = new InferSchemaAccessStrategy<>(sourceFactory, inference, mock(ComponentLog.class));
        final InputStream input = new InputStream() {
            private final ByteArrayInputStream delegate = new ByteArrayInputStream(new byte[0]);

            @Override
            public int read() {
                return delegate.read();
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public synchronized void mark(final int readLimit) {
                delegate.mark(readLimit);
            }

            @Override
            public synchronized void reset() throws IOException {
                throw resetFailure;
            }
        };

        final IOException thrown = assertThrows(IOException.class, () -> strategy.getSchema(null, input, null));

        assertSame(inferenceFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(resetFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void testSharedInferenceAndResetFailureDoesNotSelfSuppress() {
        final IOException sharedFailure = new IOException("shared failure");
        final InputStream input = new InputStream() {
            @Override
            public int read() throws IOException {
                throw sharedFailure;
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public synchronized void reset() throws IOException {
                throw sharedFailure;
            }
        };

        final IOException thrown = assertThrows(IOException.class,
                () -> RewindableInputStreamAccess.readAndReset(input, InputStream::read));

        assertSame(sharedFailure, thrown);
        assertEquals(0, thrown.getSuppressed().length);
    }

    @Test
    void testInferenceFailureIsNotMaskedByRuntimeResetFailure() {
        final IOException inferenceFailure = new IOException("inference failed");
        final IllegalStateException resetFailure = new IllegalStateException("reset failed");
        final InputStream input = new InputStream() {
            @Override
            public int read() {
                return -1;
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public synchronized void reset() {
                throw resetFailure;
            }
        };

        final IOException thrown = assertThrows(IOException.class,
                () -> RewindableInputStreamAccess.readAndReset(input, ignored -> {
                    throw inferenceFailure;
                }));

        assertSame(inferenceFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(resetFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void testRuntimeResetFailurePropagatedAfterSuccessfulOperation() {
        final IllegalStateException resetFailure = new IllegalStateException("reset failed");
        final InputStream input = new InputStream() {
            @Override
            public int read() {
                return -1;
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public synchronized void reset() {
                throw resetFailure;
            }
        };

        final IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> RewindableInputStreamAccess.readAndReset(input, ignored -> 42));

        assertSame(resetFailure, thrown);
    }

    @Test
    void testOnlyExactByteArrayInputStreamUsesNativeReset() {
        final class ByteArrayInputStreamSubclass extends ByteArrayInputStream {
            private ByteArrayInputStreamSubclass() {
                super(new byte[0]);
            }
        }
        final class NonMarkInputStream extends InputStream {
            @Override
            public int read() {
                return -1;
            }
        }

        assertTrue(RewindableInputStreamAccess.requiresReplay(new NonMarkInputStream()));
        assertTrue(RewindableInputStreamAccess.requiresReplay(new ByteArrayInputStreamSubclass()));
        assertFalse(RewindableInputStreamAccess.requiresReplay(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    void testTaskTerminationWrapperRequiresVerifiedRewindableDelegate() {
        final InputStream rewindable = new TaskTerminationInputStream(new ByteArrayInputStream(new byte[0]));
        final InputStream unverified = new TaskTerminationInputStream(new ByteArrayInputStream(new byte[0]) { });

        assertFalse(RewindableInputStreamAccess.requiresReplay(rewindable));
        assertTrue(RewindableInputStreamAccess.requiresReplay(unverified));
    }
}
