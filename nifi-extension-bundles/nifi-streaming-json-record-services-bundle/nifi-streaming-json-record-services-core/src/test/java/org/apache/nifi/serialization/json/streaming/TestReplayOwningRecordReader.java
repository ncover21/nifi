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

import org.apache.nifi.serialization.RecordReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class TestReplayOwningRecordReader {
    @Test
    void testRuntimeDelegateCloseFailureDoesNotSkipReplayCleanup() throws IOException {
        final RecordReader delegate = mock(RecordReader.class);
        final IllegalStateException delegateFailure = new IllegalStateException("delegate close failed");
        doThrow(delegateFailure).when(delegate).close();
        final CloseTrackingInputStream source = new CloseTrackingInputStream();
        final ReplayableInputStream replayable = new ReplayableInputStream(source);
        final ReplayOwningRecordReader reader = new ReplayOwningRecordReader(delegate, replayable);

        final IllegalStateException failure = assertThrows(IllegalStateException.class, reader::close);

        assertSame(delegateFailure, failure);
        assertTrue(source.closed);
    }

    private static final class CloseTrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        private CloseTrackingInputStream() {
            super(new byte[0]);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
