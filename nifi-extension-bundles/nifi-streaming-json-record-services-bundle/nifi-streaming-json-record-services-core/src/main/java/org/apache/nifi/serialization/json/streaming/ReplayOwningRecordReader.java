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

import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;

final class ReplayOwningRecordReader implements RecordReader {
    private final RecordReader delegate;
    private final ReplayableInputStream replayable;
    private boolean closeRequested;
    private boolean delegateClosed;
    private boolean replayClosed;

    ReplayOwningRecordReader(final RecordReader delegate, final ReplayableInputStream replayable) {
        this.delegate = delegate;
        this.replayable = replayable;
    }

    @Override
    public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) throws IOException, MalformedRecordException {
        if (closeRequested) {
            throw new IOException("Record Reader is closed");
        }
        return delegate.nextRecord(coerceTypes, dropUnknownFields);
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        return delegate.getSchema();
    }

    @Override
    public void close() throws IOException {
        closeRequested = true;
        Throwable failure = null;
        if (!delegateClosed) {
            try {
                delegate.close();
                delegateClosed = true;
            } catch (final IOException | RuntimeException | Error e) {
                failure = e;
            }
        }
        if (!replayClosed) {
            try {
                replayable.close();
                replayClosed = true;
            } catch (final IOException | RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throwFailure(failure);
        }
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
