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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStreamingJsonSchemaInference {
    private static final TimeValueInference TIME_VALUE_INFERENCE =
            new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @ParameterizedTest
    @ValueSource(strings = {
            "data-types.json",
            "choice-of-array-empty-or-array-record.json",
            "empty-arrays.json",
            "nested-choice-of-record-array-or-string.json",
            "nested-choice-of-empty-array-or-string.json",
            "prov-events.json",
            "docs-example.json"
    })
    void testMatchesTreeInferenceForFixtures(final String fileName) throws IOException {
        final byte[] json = Files.readAllBytes(Path.of("src/test/resources/json", fileName));
        assertEquivalent(json, StartingFieldStrategy.ROOT_NODE, null, ParsingStrategy.STANDARD);
    }

    @ParameterizedTest
    @MethodSource("nestedFieldInputs")
    void testMatchesTreeInferenceForNestedFields(final String fileName, final String startingFieldName) throws IOException {
        final byte[] json = Files.readAllBytes(Path.of("src/test/resources/json", fileName));
        assertEquivalent(json, StartingFieldStrategy.NESTED_FIELD, startingFieldName, ParsingStrategy.STANDARD);
    }

    private static Stream<Arguments> nestedFieldInputs() {
        return Stream.of(
                Arguments.of("single-element-nested-array.json", "accounts"),
                Arguments.of("single-element-nested.json", "account"),
                Arguments.of("single-element-nested-array.json", "name"),
                Arguments.of("single-element-nested-array-middle.json", "accounts"),
                Arguments.of("single-element-nested-array.json", "notfound"),
                Arguments.of("multiple-nested-field.json", "accountIds")
        );
    }

    @Test
    void testMatchesTreeInferenceForNumericBoundariesAndDuplicateFields() throws IOException {
        final String json = """
                [{"int":2147483647,"long":2147483648,"bigint":9223372036854775808,
                  "double":1.25,"nullValue":null,"value":1,"value":"last",
                  "nested":{"field":false,"field":[null]},"empty":[],"unicode":"Jöhn"},
                 {"int":-2147483648,"long":-2147483649,"nested":{"field":{"id":1}}}]
                """;
        assertEquivalent(json.getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, ParsingStrategy.STANDARD);
    }

    @Test
    void testMatchesTreeInferenceForJsonLines() throws IOException {
        final String json = "{" + "\"id\":1}\n{\"id\":2,\"optional\":true}";
        assertEquivalent(json.getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, ParsingStrategy.STANDARD);
    }

    @Test
    void testMatchesTreeInferenceForLenientJson() throws IOException {
        final String json = "[/*comment*/ {'id':+01,'values':[1,,3],}, # yaml comment\n {'id':2}]";
        assertEquivalent(json.getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, ParsingStrategy.LENIENT);
    }

    @Test
    void testMalformedInputFails() {
        final byte[] malformed = "{\"id\":1".getBytes(StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> inferStreaming(malformed, StartingFieldStrategy.ROOT_NODE, null, ParsingStrategy.STANDARD));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[{\"a\":1,\"b\":2},{\"a\":1,\"c\":3}]",
            "{\"a\":1,\"b\":2}\n{\"a\":1,\"c\":3}"
    })
    void testCumulativeRootFieldLimitAcrossRecords(final String json) {
        final IOException failure = assertThrows(IOException.class, () -> inferStreaming(json, 2));

        assertTrue(failure.getMessage().contains("field limit of 2"));
    }

    @Test
    void testCumulativeNestedFieldLimitAcrossRecords() {
        final String json = "[{\"nested\":{\"a\":1}},{\"nested\":{\"b\":2}},{\"nested\":{\"c\":3}}]";

        final IOException failure = assertThrows(IOException.class, () -> inferStreaming(json, 2));

        assertTrue(failure.getMessage().contains("field limit of 2"));
    }

    @Test
    void testCumulativeNestedArrayElementFieldLimit() {
        final String json = "{\"values\":[{\"a\":1},{\"b\":2},{\"c\":3}]}";

        final IOException failure = assertThrows(IOException.class, () -> inferStreaming(json, 2));

        assertTrue(failure.getMessage().contains("field limit of 2"));
    }

    @Test
    void testDuplicateFieldAtLimitIsAccepted() throws IOException {
        final RecordSchema schema = inferStreaming("{\"a\":1,\"b\":2,\"a\":3}", 2);

        assertEquals(List.of("a", "b"), schema.getFieldNames());
    }

    @Test
    void testCumulativeSiblingSchemaCardinalityLimit() {
        final String json = "{\"left\":{\"a\":1,\"b\":2},\"right\":{\"c\":3,\"d\":4}}";

        final IOException failure = assertThrows(IOException.class, () -> inferStreaming(json, 5));

        assertTrue(failure.getMessage().contains("field limit of 5"));
    }

    @Test
    void testStableNestedFieldsAcrossRecordsCountOnce() throws IOException {
        final RecordSchema schema = inferStreaming("[{\"nested\":{\"a\":1}},{\"nested\":{\"a\":2}}]", 2);

        assertEquals(List.of("nested"), schema.getFieldNames());
    }

    @Test
    void testStableArrayElementFieldsCountOnce() throws IOException {
        final RecordSchema schema = inferStreaming("{\"items\":[{\"a\":1},{\"a\":2}]}", 2);

        assertEquals(List.of("items"), schema.getFieldNames());
    }

    @Test
    void testRecordMetadata() throws IOException {
        final byte[] json = " [ {\"id\":1},\n {\"id\":2,\"value\":null} ] ".getBytes(StandardCharsets.UTF_8);
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.STANDARD);
        final StreamingJsonSchemaInference.InferredJsonSchema inferred = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE)
                .inferSchemaWithMetadata(new JsonParserRecordSource(json, StartingFieldStrategy.ROOT_NODE, null, parserFactory));

        assertEquals(2, inferred.records().size());
        final StreamingJsonSchemaInference.JsonRecordMetadata first = inferred.records().getFirst();
        final StreamingJsonSchemaInference.JsonRecordMetadata second = inferred.records().getLast();
        assertEquals("{\"id\":1}", slice(json, first));
        assertFalse(first.containsLineBreak());
        assertFalse(first.containsScientificNotation());
        assertTrue(first.hasObjectMembers());
        assertFalse(first.containsDuplicateFields());
        assertEquals("{\"id\":2,\"value\":null}", slice(json, second));
        assertFalse(second.containsLineBreak());
        assertFalse(second.containsScientificNotation());
        assertTrue(second.hasObjectMembers());
        assertFalse(second.containsDuplicateFields());
    }

    @Test
    void testDuplicateFieldMetadataIncludesNestedObjects() throws IOException {
        final byte[] json = "{\"id\":1,\"child\":{\"value\":1,\"value\":2}}".getBytes(StandardCharsets.UTF_8);
        final StreamingJsonSchemaInference.InferredJsonSchema inferred = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE)
                .inferSchemaWithMetadata(new JsonParserRecordSource(json, StartingFieldStrategy.ROOT_NODE, null, new StreamingJsonParserFactory()));

        assertTrue(inferred.records().getFirst().containsDuplicateFields());
    }

    @Test
    void testScientificNotationMetadataIgnoresStrings() throws IOException {
        final byte[] json = "[{\"text\":\"1e3\",\"number\":1000.0},{\"text\":\"plain\",\"number\":1e3}]".getBytes(StandardCharsets.UTF_8);
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.STANDARD);
        final StreamingJsonSchemaInference.InferredJsonSchema inferred = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE)
                .inferSchemaWithMetadata(new JsonParserRecordSource(json, StartingFieldStrategy.ROOT_NODE, null, parserFactory));

        assertFalse(inferred.records().getFirst().containsScientificNotation());
        assertTrue(inferred.records().getLast().containsScientificNotation());
    }

    @Test
    void testMetadataLimit() throws IOException {
        final byte[] json = "[{\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8);
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.STANDARD);
        final StreamingJsonSchemaInference.InferredJsonSchema inferred = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE)
                .inferSchemaWithMetadata(new JsonParserRecordSource(json, StartingFieldStrategy.ROOT_NODE, null, parserFactory), 1);

        assertFalse(inferred.metadataComplete());
        assertTrue(inferred.records().isEmpty());
    }

    @Test
    void testMetadataLimitBoundaries() throws IOException {
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(
                com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.STANDARD);
        final StreamingJsonSchemaInference inference = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE);

        final StreamingJsonSchemaInference.InferredJsonSchema empty = inference.inferSchemaWithMetadata(
                new JsonParserRecordSource("[]".getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, parserFactory), 1);
        final StreamingJsonSchemaInference.InferredJsonSchema single = inference.inferSchemaWithMetadata(
                new JsonParserRecordSource("[{\"id\":1}]".getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, parserFactory), 1);
        final StreamingJsonSchemaInference.InferredJsonSchema atLimit = inference.inferSchemaWithMetadata(
                new JsonParserRecordSource("[{\"id\":1},{\"id\":2}]".getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, parserFactory), 2);

        assertTrue(empty.metadataComplete());
        assertTrue(empty.records().isEmpty());
        assertTrue(single.metadataComplete());
        assertEquals(1, single.records().size());
        assertTrue(atLimit.metadataComplete());
        assertEquals(2, atLimit.records().size());
    }

    @Test
    void testSchemaOnlyInferenceDoesNotInspectRecordLocationsOrFloatText() throws IOException {
        final JsonParser delegate = new JsonFactory().createParser("{\"number\":1.2e3}");
        delegate.nextToken();
        final LocationTrackingParser parser = new LocationTrackingParser(delegate);
        final boolean[] supplied = {false};

        new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE).inferSchema(() -> {
            if (supplied[0]) {
                return null;
            }
            supplied[0] = true;
            return parser;
        });

        assertEquals(0, parser.locationReads);
        assertEquals(0, parser.textCharacterReads);
    }

    @Test
    void testMetadataLimitStopsInspectingLocationsAndFloatText() throws IOException {
        final LocationTrackingParser first = createTrackingParser("{\"number\":1.2e3}");
        final LocationTrackingParser beyondLimit = createTrackingParser("{\"number\":2.3e4}");
        final JsonParser[] parsers = {first, beyondLimit};
        final int[] index = {0};

        final StreamingJsonSchemaInference.InferredJsonSchema inferred = new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE)
                .inferSchemaWithMetadata(() -> index[0] == parsers.length ? null : parsers[index[0]++], 1);

        assertFalse(inferred.metadataComplete());
        assertTrue(inferred.records().isEmpty());
        assertTrue(first.locationReads > 0);
        assertTrue(first.textCharacterReads > 0);
        assertEquals(0, beyondLimit.locationReads);
        assertEquals(0, beyondLimit.textCharacterReads);
    }

    @Test
    void testNestedFieldInferenceClosesParser() throws IOException {
        final boolean[] parserClosed = {false};
        final TokenParserFactory parserFactory = input -> new JsonParserDelegate(new JsonFactory().createParser(input)) {
            @Override
            public void close() throws IOException {
                parserClosed[0] = true;
                super.close();
            }
        };
        final byte[] json = "{\"records\":[{\"id\":1}],\"tail\":true}".getBytes(StandardCharsets.UTF_8);

        new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE).inferSchema(
                new JsonParserRecordSource(json, StartingFieldStrategy.NESTED_FIELD, "records", parserFactory));

        assertTrue(parserClosed[0]);
    }

    @Test
    void testNestedFieldConstructionFailureClosesParser() {
        final IOException scanFailure = new IOException("nested field scan failed");
        final boolean[] parserClosed = {false};
        final TokenParserFactory parserFactory = input -> new JsonParserDelegate(new JsonFactory().createParser(input)) {
            @Override
            public boolean nextFieldName(final SerializableString fieldName) throws IOException {
                throw scanFailure;
            }

            @Override
            public void close() throws IOException {
                parserClosed[0] = true;
                super.close();
            }
        };

        final IOException thrown = assertThrows(IOException.class, () -> new JsonParserRecordSource(
                new ByteArrayInputStream("{\"records\":[]}".getBytes(StandardCharsets.UTF_8)),
                StartingFieldStrategy.NESTED_FIELD, "records", parserFactory));

        assertSame(scanFailure, thrown);
        assertTrue(parserClosed[0]);
    }

    @Test
    void testNestedFieldConstructionFailureIsNotMaskedByRuntimeCloseFailure() {
        final IOException scanFailure = new IOException("nested field scan failed");
        final IllegalStateException closeFailure = new IllegalStateException("close failed");
        final TokenParserFactory parserFactory = input -> new JsonParserDelegate(new JsonFactory().createParser(input)) {
            @Override
            public boolean nextFieldName(final SerializableString fieldName) throws IOException {
                throw scanFailure;
            }

            @Override
            public void close() {
                throw closeFailure;
            }
        };

        final IOException thrown = assertThrows(IOException.class, () -> new JsonParserRecordSource(
                new ByteArrayInputStream("{\"records\":[]}".getBytes(StandardCharsets.UTF_8)),
                StartingFieldStrategy.NESTED_FIELD, "records", parserFactory));

        assertSame(scanFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
    }

    @Test
    void testInferenceFailureIsNotMaskedByCloseFailure() {
        final IOException inferenceFailure = new IOException("inference failed");
        final IOException closeFailure = new IOException("close failed");
        final RecordSource<JsonParser> source = new FailingCloseRecordSource(inferenceFailure, closeFailure);

        final IOException thrown = assertThrows(IOException.class,
                () -> new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE).inferSchema(source));

        assertSame(inferenceFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
    }

    private void assertEquivalent(final byte[] json, final StartingFieldStrategy strategy, final String startingFieldName,
            final ParsingStrategy parsingStrategy) throws IOException {
        final RecordSchema expected = inferTree(json, strategy, startingFieldName, parsingStrategy);
        final RecordSchema actual = inferStreaming(json, strategy, startingFieldName, parsingStrategy);
        assertEquals(expected, actual);
        assertEquals(expected.getFieldNames(), actual.getFieldNames());
    }

    private RecordSchema inferTree(final byte[] json, final StartingFieldStrategy strategy, final String startingFieldName,
            final ParsingStrategy parsingStrategy) throws IOException {
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(com.fasterxml.jackson.core.StreamReadConstraints.defaults(), parsingStrategy);
        return new JsonSchemaInference(TIME_VALUE_INFERENCE).inferSchema(
                new JsonRecordSource(new ByteArrayInputStream(json), strategy, startingFieldName, parserFactory));
    }

    private RecordSchema inferStreaming(final byte[] json, final StartingFieldStrategy strategy, final String startingFieldName,
            final ParsingStrategy parsingStrategy) throws IOException {
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(com.fasterxml.jackson.core.StreamReadConstraints.defaults(), parsingStrategy);
        return new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE).inferSchema(
                new JsonParserRecordSource(new ByteArrayInputStream(json), strategy, startingFieldName, parserFactory));
    }

    private RecordSchema inferStreaming(final String json, final int maximumFields) throws IOException {
        final StreamingJsonParserFactory parserFactory = new StreamingJsonParserFactory(
                com.fasterxml.jackson.core.StreamReadConstraints.defaults(), ParsingStrategy.STANDARD);
        return new StreamingJsonSchemaInference(TIME_VALUE_INFERENCE, maximumFields).inferSchema(
                new JsonParserRecordSource(json.getBytes(StandardCharsets.UTF_8), StartingFieldStrategy.ROOT_NODE, null, parserFactory));
    }

    private String slice(final byte[] json, final StreamingJsonSchemaInference.JsonRecordMetadata metadata) {
        return new String(json, Math.toIntExact(metadata.startOffset()), Math.toIntExact(metadata.endOffset() - metadata.startOffset()), StandardCharsets.UTF_8);
    }

    private LocationTrackingParser createTrackingParser(final String json) throws IOException {
        final JsonParser delegate = new JsonFactory().createParser(json);
        delegate.nextToken();
        return new LocationTrackingParser(delegate);
    }

    private static final class LocationTrackingParser extends JsonParserDelegate {
        private int locationReads;
        private int textCharacterReads;

        private LocationTrackingParser(final JsonParser delegate) {
            super(delegate);
        }

        @Override
        public JsonLocation currentTokenLocation() {
            locationReads++;
            return super.currentTokenLocation();
        }

        @Override
        public JsonLocation currentLocation() {
            locationReads++;
            return super.currentLocation();
        }

        @Override
        public char[] getTextCharacters() throws IOException {
            textCharacterReads++;
            return super.getTextCharacters();
        }
    }

    private static final class FailingCloseRecordSource implements RecordSource<JsonParser>, AutoCloseable {
        private final IOException inferenceFailure;
        private final IOException closeFailure;

        private FailingCloseRecordSource(final IOException inferenceFailure, final IOException closeFailure) {
            this.inferenceFailure = inferenceFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public JsonParser next() throws IOException {
            throw inferenceFailure;
        }

        @Override
        public void close() throws IOException {
            throw closeFailure;
        }
    }
}
