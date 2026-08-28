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
package org.apache.nifi.json.benchmark;

import java.nio.charset.StandardCharsets;

final class KafkaJsonFixture {
    private KafkaJsonFixture() {
    }

    static byte[] create(final long sequence, final int targetRecordBytes, final int nullPercentage, final boolean drifting) {
        final String baseJson = """
                {"id":%d,"deviceId":"device-%04d","timestamp":"2026-08-27T10:15:30.123Z",\
                "active":true,"temperature":72.125,"metrics":{"cpu":0.42,"memory":7340032,"disk":0.81},\
                "tags":["production","west","sensor"],"samples":[%s,13,15,18,21,34],\
                "message":"The quick brown fox jumps over the lazy dog while carrying a representative Kafka event payload",\
                "location":{"latitude":37.7749,"longitude":-122.4194,"region":"us-west"}}
                """.formatted(sequence, sequence % 10_000, sequence % 100 < nullPercentage ? "null" : "12").strip();
        final String withDrift = drifting && hasFirmwareVersion(sequence)
                ? baseJson.substring(0, baseJson.length() - 1) + ",\"firmwareVersion\":\"2026.08\"}"
                : baseJson;
        final int paddingLength = targetRecordBytes - withDrift.getBytes(StandardCharsets.UTF_8).length - 13;
        final String json = paddingLength <= 0 ? withDrift
                : withDrift.substring(0, withDrift.length() - 1) + ",\"padding\":\"" + "x".repeat(paddingLength) + "\"}";
        return json.getBytes(StandardCharsets.UTF_8);
    }

    static boolean hasFirmwareVersion(final long sequence) {
        final long position = sequence % 100;
        return (position < 10 && position % 2 == 0) || (position >= 90 && position % 2 == 1);
    }
}
