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

import com.fasterxml.jackson.core.JsonParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

interface TokenParserFactory {
    /**
     * Get JSON Parser implementation for provided Input Stream with preconfigured settings
     *
     * @param in Input Stream to be parsed
     * @return JSON Parser
     * @throws IOException Thrown on failures to read the Input Stream
     */
    JsonParser getJsonParser(InputStream in) throws IOException;

    /**
     * Get a JSON Parser for an in-memory input.
     *
     * @param input bytes to parse for the lifetime of the returned parser
     * @return JSON Parser
     * @throws IOException Thrown on failures to read the input
     */
    default JsonParser getJsonParser(final byte[] input, final int offset, final int length) throws IOException {
        return getJsonParser(new ByteArrayInputStream(input, offset, length));
    }

    /**
     * @return true when accepted input is strict JSON and source bytes can be reused as JSON output
     */
    default boolean supportsSerializedJson() {
        return false;
    }

    /**
     * @return true when UTF-8 InputStream parser locations use absolute byte offsets
     */
    default boolean supportsInputStreamByteOffsets() {
        return false;
    }
}
