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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordCapturingInputStream {
    @Test
    void testPassiveCaptureRetainsOnlyLatestRead() throws IOException {
        final byte[] whitespace = new byte[8 * 1024 * 1024];
        Arrays.fill(whitespace, (byte) ' ');
        final byte[] readBuffer = new byte[4096];

        try (RecordCapturingInputStream input = new RecordCapturingInputStream(new ByteArrayInputStream(whitespace))) {
            while (input.read(readBuffer) >= 0) {
                // Read the complete prefix without starting a logical record
            }
            assertEquals(readBuffer.length + 8, input.getRetainedByteCount());
            assertEquals(readBuffer.length + 8, input.getBufferCapacity());
        }
    }

    @Test
    void testCompletedLargeRecordReleasesPeakBuffer() throws IOException {
        final byte[] record = new byte[4 * 1024 * 1024];
        record[0] = '{';
        record[record.length - 1] = '}';

        try (RecordCapturingInputStream input = new RecordCapturingInputStream(new ByteArrayInputStream(record))) {
            assertEquals('{', input.read());
            assertTrue(input.startRecord(0));
            assertEquals(record.length - 1, input.readNBytes(record.length - 1).length);
            input.finishRecord(record.length);

            assertEquals(0, input.getRetainedByteCount());
            assertTrue(input.getBufferCapacity() <= 65_536);
        }
    }

    @Test
    void testOversizedRecordAbandonsCaptureAndContinuesReading() throws IOException {
        final byte[] record = new byte[RecordCapturingInputStream.MAX_RECORD_CAPTURE_BYTES + 1];
        record[0] = '{';
        record[record.length - 1] = '}';

        try (RecordCapturingInputStream input = new RecordCapturingInputStream(new ByteArrayInputStream(record))) {
            assertEquals('{', input.read());
            assertTrue(input.startRecord(0));
            assertEquals(record.length - 1, input.readNBytes(record.length - 1).length);
            assertTrue(input.isRecordCaptureExceeded());
            assertFalse(input.contains(0, record.length));
            assertTrue(input.getBufferCapacity() <= 65_536);

            input.finishRecord(record.length);
            assertFalse(input.isRecordCaptureExceeded());
            assertEquals(0, input.getRetainedByteCount());
        }
    }
}
