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

package org.apache.nifi.serialization;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Optional capability for Record Reader factories that can retain or otherwise optimize in-memory input.
 */
public interface ByteArrayRecordReaderFactory extends RecordReaderFactory {

    /**
     * Creates a Record Reader from an in-memory byte array. The caller must not modify the array while the returned
     * reader or any records produced by it remain in use.
     *
     * @param variables variables used to resolve the Record Schema
     * @param input bytes containing Records
     * @param logger logger bound to a component
     * @return created Record Reader
     */
    default RecordReader createRecordReaderFromBytes(final Map<String, String> variables, final byte[] input, final ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        return createRecordReader(variables, new ByteArrayInputStream(input), input.length, logger);
    }
}
