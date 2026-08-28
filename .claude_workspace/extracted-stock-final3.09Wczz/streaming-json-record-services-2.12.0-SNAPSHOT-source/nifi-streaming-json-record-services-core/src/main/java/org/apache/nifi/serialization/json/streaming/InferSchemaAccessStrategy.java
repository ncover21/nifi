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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

final class InferSchemaAccessStrategy<T> implements SchemaAccessStrategy, ByteSchemaAccessStrategy {
    private final RecordSourceFactory<T> recordSourceFactory;
    private final SchemaInferenceEngine<T> schemaInference;
    private final ComponentLog logger;

    public InferSchemaAccessStrategy(final RecordSourceFactory<T> recordSourceFactory, final SchemaInferenceEngine<T> schemaInference, final ComponentLog logger) {
        this.recordSourceFactory = recordSourceFactory;
        this.schemaInference = schemaInference;
        this.logger = logger;
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws IOException {
        return RewindableInputStreamAccess.readAndReset(contentStream, inferenceInput -> {
            final RecordSource<T> recordSource = recordSourceFactory.create(variables, inferenceInput);
            final RecordSchema schema = schemaInference.inferSchema(recordSource);
            logger.debug("Successfully inferred schema {}", schema);
            return schema;
        });
    }

    public RecordSchema getSchemaFromBytes(final Map<String, String> variables, final byte[] content, final RecordSchema readSchema) throws IOException {
        final RecordSource<T> recordSource;
        if (recordSourceFactory instanceof final ByteRecordSourceFactory<?> byteSourceFactory) {
            @SuppressWarnings("unchecked")
            final RecordSource<T> byteRecordSource = (RecordSource<T>) byteSourceFactory.createFromBytes(variables, content);
            recordSource = byteRecordSource;
        } else {
            recordSource = recordSourceFactory.create(variables, new java.io.ByteArrayInputStream(content));
        }
        final RecordSchema schema = schemaInference.inferSchema(recordSource);
        logger.debug("Successfully inferred schema {}", schema);
        return schema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return EnumSet.noneOf(SchemaField.class);
    }
}
