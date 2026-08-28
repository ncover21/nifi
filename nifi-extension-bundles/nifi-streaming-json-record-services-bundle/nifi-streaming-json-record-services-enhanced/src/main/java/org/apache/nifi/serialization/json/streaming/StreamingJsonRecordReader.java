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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Tags({"json", "streaming", "record", "reader", "parser"})
@CapabilityDescription("Reads one or more JSON objects or arrays as Records using tree-free token processing. Supports the standard Record Reader InputStream "
        + "contract and enhanced direct-byte host integration, with correctness-preserving fallback for unsupported streaming cases.")
@SeeAlso(StreamingJsonRecordSetWriter.class)
public final class StreamingJsonRecordReader extends AbstractStreamingJsonRecordReaderService implements ByteArrayRecordReaderFactory {
    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream input, final long inputLength,
                                           final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        return super.createRecordReader(variables, input, inputLength, logger);
    }

    @Override
    public RecordReader createRecordReaderFromBytes(final Map<String, String> variables, final byte[] input, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        return createRecordReaderFromBytesInternal(variables, input, logger);
    }
}
