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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStreamingJsonControllerServices {
    @Test
    void testStockProviderAndContracts() {
        final Set<Class<?>> provided = ServiceLoader.load(ControllerService.class).stream()
                .map(ServiceLoader.Provider::type)
                .filter(type -> type.getPackageName().equals(StreamingJsonRecordReader.class.getPackageName()))
                .collect(Collectors.toSet());
        final StreamingJsonRecordReader reader = new StreamingJsonRecordReader();

        assertEquals(Set.of(StreamingJsonRecordReader.class, StreamingJsonRecordSetWriter.class), provided);
        assertInstanceOf(RecordReaderFactory.class, reader);
        assertFalse(Set.of(reader.getClass().getInterfaces()).stream()
                .anyMatch(type -> type.getName().equals("org.apache.nifi.serialization.ByteArrayRecordReaderFactory")));
        assertInstanceOf(RecordSetWriterFactory.class, new StreamingJsonRecordSetWriter());
        assertTrue(Modifier.isFinal(StreamingJsonRecordReader.class.getModifiers()));
        assertTrue(Modifier.isFinal(StreamingJsonRecordSetWriter.class.getModifiers()));
        assertNotNull(StreamingJsonRecordReader.class.getClassLoader().getResource(
                "docs/org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader/additionalDetails.md"));
    }
}
