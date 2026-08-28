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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.io.SerializedString;
import org.apache.nifi.schema.inference.RecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

final class JsonParserRecordSource implements RecordSource<JsonParser>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JsonParserRecordSource.class);
    private static final TokenParserFactory DEFAULT_TOKEN_PARSER_FACTORY = new StreamingJsonParserFactory(StreamReadConstraints.defaults(), ParsingStrategy.LENIENT);

    private final JsonParser jsonParser;
    private final StartingFieldStrategy strategy;

    public JsonParserRecordSource(final InputStream in) throws IOException {
        this(in, null, null, DEFAULT_TOKEN_PARSER_FACTORY);
    }

    public JsonParserRecordSource(final InputStream in, final StartingFieldStrategy strategy, final String startingFieldName,
            final TokenParserFactory tokenParserFactory) throws IOException {
        this(tokenParserFactory.getJsonParser(in), strategy, startingFieldName);
    }

    public JsonParserRecordSource(final byte[] input, final StartingFieldStrategy strategy, final String startingFieldName,
            final TokenParserFactory tokenParserFactory) throws IOException {
        this(tokenParserFactory.getJsonParser(input, 0, input.length), strategy, startingFieldName);
    }

    private JsonParserRecordSource(final JsonParser jsonParser, final StartingFieldStrategy strategy, final String startingFieldName) throws IOException {
        this.jsonParser = jsonParser;
        this.strategy = strategy;

        try {
            if (strategy == StartingFieldStrategy.NESTED_FIELD) {
                final SerializedString serializedNestedField = new SerializedString(startingFieldName);
                while (!jsonParser.nextFieldName(serializedNestedField) && jsonParser.hasCurrentToken()) {
                    // Continue until the configured field or the end of input
                }
                logger.debug("Parsing starting at nested field [{}]", startingFieldName);
            }
        } catch (final IOException | RuntimeException | Error e) {
            try {
                jsonParser.close();
            } catch (final Throwable closeFailure) {
                if (closeFailure != e) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
    }

    @Override
    public JsonParser next() throws IOException {
        while (true) {
            final JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return null;
            }

            if (token == JsonToken.START_OBJECT) {
                return jsonParser;
            }

            if (strategy == StartingFieldStrategy.NESTED_FIELD && (token == JsonToken.END_ARRAY || token == JsonToken.END_OBJECT)) {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        jsonParser.close();
    }
}
