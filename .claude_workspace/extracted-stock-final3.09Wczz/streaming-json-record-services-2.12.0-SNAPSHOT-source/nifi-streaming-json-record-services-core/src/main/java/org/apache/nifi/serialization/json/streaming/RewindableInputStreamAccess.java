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

import org.apache.nifi.stream.io.NonCloseableInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

final class RewindableInputStreamAccess {
    private static final int READ_LIMIT = 1_000_000;

    private RewindableInputStreamAccess() {
    }

    static boolean requiresReplay(final InputStream input) {
        if (!input.markSupported()) {
            return true;
        }
        if (input.getClass() == ByteArrayInputStream.class || input instanceof ReplayableInputStream) {
            return false;
        }

        final String className = input.getClass().getName();
        if (className.equals("org.apache.nifi.controller.repository.io.ContentClaimInputStream")) {
            return false;
        }
        if (className.equals("org.apache.nifi.controller.repository.io.TaskTerminationInputStream")) {
            return !hasRewindableTaskDelegate(input);
        }
        return true;
    }

    private static boolean hasRewindableTaskDelegate(final InputStream input) {
        try {
            final Field delegateField = input.getClass().getDeclaredField("delegate");
            if (!delegateField.trySetAccessible()) {
                return false;
            }
            final Object delegate = delegateField.get(input);
            if (!(delegate instanceof final InputStream delegateInput)) {
                return false;
            }
            return delegateInput.getClass() == ByteArrayInputStream.class
                    || delegateInput instanceof ReplayableInputStream
                    || delegateInput.getClass().getName().equals("org.apache.nifi.controller.repository.io.ContentClaimInputStream");
        } catch (final ReflectiveOperationException | RuntimeException e) {
            return false;
        }
    }

    static <T> T readAndReset(final InputStream input, final InputOperation<T> operation) throws IOException {
        if (!input.markSupported()) {
            throw new IOException("Schema inference requires a rewindable InputStream with mark/reset support");
        }

        input.mark(READ_LIMIT);
        Throwable operationFailure = null;
        try {
            return operation.read(new NonCloseableInputStream(input));
        } catch (final IOException | RuntimeException | Error e) {
            operationFailure = e;
            throw e;
        } finally {
            try {
                input.reset();
            } catch (final IOException | RuntimeException | Error resetFailure) {
                if (operationFailure == null) {
                    throw resetFailure;
                }
                if (operationFailure != resetFailure) {
                    operationFailure.addSuppressed(resetFailure);
                }
            }
        }
    }

    @FunctionalInterface
    interface InputOperation<T> {
        T read(InputStream input) throws IOException;
    }
}
