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
import com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSchemaCacheService;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.serialization.json.streaming.SchemaInferenceUtil.INFER_SCHEMA;
import static org.apache.nifi.serialization.json.streaming.SchemaInferenceUtil.OBSOLETE_SCHEMA_CACHE;
import static org.apache.nifi.serialization.json.streaming.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"json", "streaming", "record", "reader", "parser"})
@CapabilityDescription("Parses JSON into individual Record objects. While the reader expects each record "
        + "to be well-formed JSON, the content of a FlowFile may consist of many records, each as a well-formed "
        + "JSON array or JSON object with optional whitespace between them, such as the common 'JSON-per-line' format. "
        + "If an array is encountered, each element in that array will be treated as a separate record. "
        + "If the schema that is configured contains a field that is not present in the JSON, a null value will be used. If the JSON contains "
        + "a field that is not present in the schema, that field will be skipped. "
        + "See the Usage of the Controller Service for more information and examples.")
abstract class AbstractStreamingJsonRecordReaderService extends SchemaRegistryService implements RecordReaderFactory {
    static final int MAX_DEFERRED_RECORDS = 1024;
    static final String DEFAULT_MAX_SCHEMA_INFERENCE_REPLAY_SIZE = "1 GB";
    static final int DEFAULT_MAX_SCHEMA_INFERENCE_FIELDS = 10_000;
    static final int DEFAULT_MAX_NESTING_DEPTH = 1_000;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile String startingFieldName;
    private volatile StartingFieldStrategy startingFieldStrategy;
    private volatile SchemaApplicationStrategy schemaApplicationStrategy;
    private volatile RecordMaterializationStrategy recordMaterializationStrategy;
    private volatile TokenParserFactory tokenParserFactory;
    private volatile boolean directSchemaInference;
    private volatile boolean inferredSchemaAccess;
    private volatile boolean contentEncodedSchemaReference;
    private volatile boolean deferredFallbackLogged;
    private volatile long maxSchemaInferenceReplayBytes;
    private volatile int maxSchemaInferenceFields;
    private volatile StreamingJsonSchemaInference streamingSchemaInference;

    public static final PropertyDescriptor STARTING_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("Starting Field Strategy")
            .description("Start processing from the root node or from a specified nested node.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(StartingFieldStrategy.ROOT_NODE.getValue())
            .allowableValues(StartingFieldStrategy.class)
            .build();

    public static final PropertyDescriptor STARTING_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Starting Field Name")
            .description("Skips forward to the given nested field (array or object) to begin processing.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .build();

    public static final PropertyDescriptor SCHEMA_APPLICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Schema Application Strategy")
            .description("Specifies whether the schema is defined for the whole document or for the selected part starting from \"Starting Field Name\".")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(SchemaApplicationStrategy.SELECTED_PART.getValue())
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, SCHEMA_REFERENCE_READER_PROPERTY)
            .allowableValues(SchemaApplicationStrategy.class)
            .build();

    public static final PropertyDescriptor RECORD_MATERIALIZATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Record Materialization Strategy")
            .description("Specifies when JSON fields are converted into typed Record values. Deferred is optimized for pass-through and grouping. "
                    + "Eager is optimized for flows that inspect or modify most Records. Deferred is preferred for eligible direct inferred-schema inputs "
                    + "and falls back to eager conversion when its encoding, parser, schema, or bounded metadata requirements are not met.")
            .required(true)
            .defaultValue(RecordMaterializationStrategy.DEFERRED.getValue())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
            .allowableValues(RecordMaterializationStrategy.class)
            .build();

    public static final PropertyDescriptor MAX_SCHEMA_INFERENCE_REPLAY_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Schema Inference Replay Size")
            .description("Maximum content retained per Record Reader when inferred-schema processing receives an InputStream without the NiFi "
                    + "ProcessSession rewind contract. Replay uses memory for up to 1 MiB and then a delete-on-close temporary file. Lower this "
                    + "limit to constrain concurrent temporary storage, or use a static or cached schema for larger generic streams.")
            .required(true)
            .defaultValue(DEFAULT_MAX_SCHEMA_INFERENCE_REPLAY_SIZE)
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
            .build();

    public static final PropertyDescriptor MAX_SCHEMA_INFERENCE_FIELDS = new PropertyDescriptor.Builder()
            .name("Maximum Schema Inference Fields")
            .description("Maximum number of distinct fields retained across the complete inferred schema, including nested records and array "
                    + "elements. Increase the value only when schemas legitimately contain more fields, or use an explicit schema.")
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_MAX_SCHEMA_INFERENCE_FIELDS))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
            .build();

    public static final PropertyDescriptor MAX_NESTING_DEPTH = new PropertyDescriptor.Builder()
            .name("Maximum JSON Nesting Depth")
            .description("Maximum JSON object and array nesting depth accepted by the parser. The default matches Jackson's standard limit. "
                    + "Lower values provide a stricter recursion bound for untrusted input.")
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_MAX_NESTING_DEPTH))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_CACHE)
                .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
                .build());
        properties.add(STARTING_FIELD_STRATEGY);
        properties.add(STARTING_FIELD_NAME);
        properties.add(SCHEMA_APPLICATION_STRATEGY);
        properties.add(AbstractJsonRowRecordReader.MAX_STRING_LENGTH);
        properties.add(AbstractJsonRowRecordReader.PARSING_STRATEGY);
        properties.add(RECORD_MATERIALIZATION_STRATEGY);
        properties.add(MAX_SCHEMA_INFERENCE_REPLAY_SIZE);
        properties.add(MAX_SCHEMA_INFERENCE_FIELDS);
        properties.add(MAX_NESTING_DEPTH);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @OnEnabled
    public void storePropertyValues(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.startingFieldStrategy = StartingFieldStrategy.valueOf(context.getProperty(STARTING_FIELD_STRATEGY).getValue());
        this.startingFieldName = context.getProperty(STARTING_FIELD_NAME).getValue();
        this.schemaApplicationStrategy = SchemaApplicationStrategy.valueOf(context.getProperty(SCHEMA_APPLICATION_STRATEGY).getValue());
        this.recordMaterializationStrategy = context.getProperty(RECORD_MATERIALIZATION_STRATEGY)
                .asAllowableValue(RecordMaterializationStrategy.class);
        this.maxSchemaInferenceReplayBytes = context.getProperty(MAX_SCHEMA_INFERENCE_REPLAY_SIZE).asDataSize(DataUnit.B).longValue();
        this.maxSchemaInferenceFields = context.getProperty(MAX_SCHEMA_INFERENCE_FIELDS).asInteger();
        this.tokenParserFactory = createTokenParserFactory(context);
        this.streamingSchemaInference = new StreamingJsonSchemaInference(
                new TimeValueInference(dateFormat, timeFormat, timestampFormat), maxSchemaInferenceFields);
        final String schemaAccessStrategy = context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        this.inferredSchemaAccess = INFER_SCHEMA.getValue().equalsIgnoreCase(schemaAccessStrategy);
        this.directSchemaInference = inferredSchemaAccess
                && context.getProperty(SCHEMA_CACHE).asControllerService(RecordSchemaCacheService.class) == null;
        this.contentEncodedSchemaReference = SCHEMA_REFERENCE_READER_PROPERTY.getValue().equalsIgnoreCase(schemaAccessStrategy);
        this.deferredFallbackLogged = false;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("starting-field-strategy", STARTING_FIELD_STRATEGY.getName());
        config.renameProperty("starting-field-name", STARTING_FIELD_NAME.getName());
        config.renameProperty("schema-application-strategy", SCHEMA_APPLICATION_STRATEGY.getName());
        config.renameProperty(OBSOLETE_SCHEMA_CACHE, SCHEMA_CACHE.getName());

        if (config.isPropertySet(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS)) {
            final String allowCommentsRawValue = config.getRawPropertyValue(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS).orElse(Boolean.FALSE.toString());
            final boolean allowComments = Boolean.parseBoolean(allowCommentsRawValue);
            if (allowComments) {
                config.setProperty(AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.LENIENT.getValue());
            } else {
                config.setProperty(AbstractJsonRowRecordReader.PARSING_STRATEGY, ParsingStrategy.STANDARD.getValue());
            }

            config.removeProperty(AbstractJsonRowRecordReader.OBSOLETE_ALLOW_COMMENTS);
        }
    }

    private TokenParserFactory createTokenParserFactory(final ConfigurationContext context) {
        final ParsingStrategy parsingStrategy =
                context.getProperty(AbstractJsonRowRecordReader.PARSING_STRATEGY).asAllowableValue(ParsingStrategy.class);
        return new StreamingJsonParserFactory(buildStreamReadConstraints(context), parsingStrategy);
    }

    /**
     * Build Stream Read Constraints based on available properties
     *
     * @param context Configuration Context with property values
     * @return Stream Read Constraints
     */
    private StreamReadConstraints buildStreamReadConstraints(final ConfigurationContext context) {
        final int maxStringLength = context.getProperty(AbstractJsonRowRecordReader.MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        final int maxNestingDepth = context.getProperty(MAX_NESTING_DEPTH).asInteger();
        return StreamReadConstraints.builder().maxStringLength(maxStringLength).maxNestingDepth(maxNestingDepth).build();
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(INFER_SCHEMA);
        allowableValues.addAll(super.getSchemaAccessStrategyValues());
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String schemaAccessStrategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        final RecordSourceFactory<JsonParser> jsonSourceFactory = createJsonParserRecordSourceFactory();
        final Supplier<SchemaInferenceEngine<JsonParser>> inferenceSupplier =
                () -> new StreamingJsonSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat),
                        context.getProperty(MAX_SCHEMA_INFERENCE_FIELDS).asInteger());

        return SchemaInferenceUtil.getSchemaAccessStrategy(schemaAccessStrategy, context, getLogger(), jsonSourceFactory, inferenceSupplier,
                () -> super.getSchemaAccessStrategy(schemaAccessStrategy, schemaRegistry, context));
    }

    private ByteRecordSourceFactory<JsonParser> createJsonParserRecordSourceFactory() {
        return new ByteRecordSourceFactory<>() {
            @Override
            public RecordSource<JsonParser> create(final Map<String, String> variables, final InputStream contentStream) throws IOException {
                return new JsonParserRecordSource(contentStream, startingFieldStrategy, startingFieldName, tokenParserFactory);
            }

            public RecordSource<JsonParser> createFromBytes(final Map<String, String> variables, final byte[] content) throws IOException {
                return new JsonParserRecordSource(content, startingFieldStrategy, startingFieldName, tokenParserFactory);
            }
        };
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        final boolean replayRequired = inferredSchemaAccess && RewindableInputStreamAccess.requiresReplay(in);
        final ReplayableInputStream replayable = replayRequired ? createReplayableInputStream(in) : null;
        final InputStream recordStream = replayable == null ? in : replayable;
        try {
            if (directSchemaInference && recordMaterializationStrategy == RecordMaterializationStrategy.DEFERRED
                    && startingFieldStrategy == StartingFieldStrategy.ROOT_NODE
                    && tokenParserFactory.supportsSerializedJson() && tokenParserFactory.supportsInputStreamByteOffsets()) {
                return retainReplayOwnership(createDirectInferenceRecordReader(recordStream, logger), replayable);
            }

            logDeferredFallback(deferredEligibilityFailure());

            final Map<String, String> normalizedVariables = normalizeVariables(variables);
            final RecordSchema schema = getSchema(normalizedVariables, recordStream, null);
            if (replayable != null) {
                replayable.completeSchemaAccess();
            }
            if (StreamingJsonRowRecordReader.isSchemaSupported(schema)) {
                final RecordReader reader = inferredSchemaAccess && recordMaterializationStrategy == RecordMaterializationStrategy.EAGER
                        ? createNonCapturingStreamingJsonRowRecordReader(recordStream, logger, schema)
                        : createStreamingJsonRowRecordReader(recordStream, logger, schema);
                return retainReplayOwnership(reader, replayable);
            }
            return retainReplayOwnership(createJsonTreeRowRecordReader(recordStream, logger, schema), replayable);
        } catch (final IOException | RuntimeException | Error e) {
            if (replayable != null) {
                try {
                    replayable.close();
                } catch (final IOException | RuntimeException | Error closeFailure) {
                    if (e != closeFailure) {
                        e.addSuppressed(closeFailure);
                    }
                }
            }
            throw e;
        }
    }

    private RecordReader createDirectInferenceRecordReader(final InputStream input, final ComponentLog logger)
            throws IOException, MalformedRecordException {
        final InputStreamInference inferredInput = RewindableInputStreamAccess.readAndReset(input, inferenceInput -> {
            final JsonEncodingProbeInputStream encodingProbe = new JsonEncodingProbeInputStream(inferenceInput);
            final JsonParserRecordSource recordSource = new JsonParserRecordSource(
                    encodingProbe, startingFieldStrategy, startingFieldName, tokenParserFactory);
            final StreamingJsonSchemaInference.InferredJsonSchema inferred =
                    streamingSchemaInference.inferSchemaWithMetadata(recordSource, MAX_DEFERRED_RECORDS);
            return new InputStreamInference(inferred, encodingProbe.isUtf8EncodedJson());
        });

        final StreamingJsonSchemaInference.InferredJsonSchema inferred = inferredInput.inferred();
        final RecordSchema schema = inferred.schema();
        getLogger().debug("Successfully inferred schema {}", schema);
        if (StreamingJsonRowRecordReader.isSchemaSupported(schema)) {
            if (inferredInput.utf8Encoded() && inferred.metadataComplete()
                    && ValidatedInputStreamRecordReader.isMetadataSupported(inferred.records())) {
                return new ValidatedInputStreamRecordReader(input, logger, schema, inferred.records(), dateFormat,
                        timeFormat, timestampFormat, tokenParserFactory);
            }
            logDeferredFallback(inferredInput.utf8Encoded() && inferred.metadataComplete()
                    ? "validated record byte limits were exceeded" : "the input encoding or inference metadata is not eligible");
            return createNonCapturingStreamingJsonRowRecordReader(input, logger, schema);
        }
        logDeferredFallback("the inferred schema requires compatibility decoding");
        return createJsonTreeRowRecordReader(input, logger, schema);
    }

    private record InputStreamInference(StreamingJsonSchemaInference.InferredJsonSchema inferred, boolean utf8Encoded) {
    }

    protected final RecordReader createRecordReaderFromBytesInternal(final Map<String, String> variables, final byte[] input, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (contentEncodedSchemaReference || !tokenParserFactory.supportsSerializedJson()
                || !Utf8JsonValue.isUtf8EncodedJson(input)) {
            return createRecordReader(variables, new ByteArrayInputStream(input), input.length, logger);
        }
        if (directSchemaInference && startingFieldStrategy == StartingFieldStrategy.ROOT_NODE) {
            final JsonParserRecordSource recordSource = new JsonParserRecordSource(input, startingFieldStrategy, startingFieldName, tokenParserFactory);
            if (recordMaterializationStrategy == RecordMaterializationStrategy.EAGER) {
                final RecordSchema schema = streamingSchemaInference.inferSchema(recordSource);
                getLogger().debug("Successfully inferred schema {}", schema);
                return StreamingJsonRowRecordReader.isSchemaSupported(schema)
                        ? createValidatedStreamingJsonRowRecordReader(input, logger, schema)
                        : createJsonTreeRowRecordReaderFromBytes(input, logger, schema);
            }
            final StreamingJsonSchemaInference.InferredJsonSchema inferred =
                    streamingSchemaInference.inferSchemaWithMetadata(recordSource, MAX_DEFERRED_RECORDS);
            final RecordSchema schema = inferred.schema();
            getLogger().debug("Successfully inferred schema {}", schema);
            if (StreamingJsonRowRecordReader.isSchemaSupported(schema)) {
                if (inferred.metadataComplete()) {
                    return new ValidatedJsonRecordReader(input, logger, schema, inferred.records(), dateFormat, timeFormat, timestampFormat, tokenParserFactory);
                }
                logDeferredFallback("inference metadata exceeded the record-count limit");
                return createValidatedStreamingJsonRowRecordReader(input, logger, schema);
            }
            logDeferredFallback("the inferred schema requires compatibility decoding");
            return createJsonTreeRowRecordReaderFromBytes(input, logger, schema);
        }

        logDeferredFallback(deferredEligibilityFailure());

        final Map<String, String> normalizedVariables = normalizeVariables(variables);
        final SchemaAccessStrategy accessStrategy = getSchemaAccessStrategy();
        if (accessStrategy == null) {
            throw new SchemaNotFoundException("Could not determine the Schema Access Strategy for this service");
        }
        final RecordSchema schema = accessStrategy instanceof final ByteSchemaAccessStrategy byteAccessStrategy
                ? byteAccessStrategy.getSchemaFromBytes(normalizedVariables, input, null)
                : accessStrategy.getSchema(normalizedVariables, new ByteArrayInputStream(input), null);
        if (StreamingJsonRowRecordReader.isSchemaSupported(schema)) {
            return createStreamingJsonRowRecordReader(input, logger, schema);
        }
        return createJsonTreeRowRecordReaderFromBytes(input, logger, schema);
    }

    private String deferredEligibilityFailure() {
        if (!inferredSchemaAccess || recordMaterializationStrategy != RecordMaterializationStrategy.DEFERRED) {
            return null;
        }
        if (!directSchemaInference) {
            return "schema caching is configured";
        }
        if (startingFieldStrategy != StartingFieldStrategy.ROOT_NODE) {
            return "a nested starting field is configured";
        }
        if (!tokenParserFactory.supportsSerializedJson() || !tokenParserFactory.supportsInputStreamByteOffsets()) {
            return "the configured parsing strategy does not provide strict UTF-8 byte offsets";
        }
        return null;
    }

    private void logDeferredFallback(final String reason) {
        if (reason == null || deferredFallbackLogged) {
            return;
        }
        synchronized (this) {
            if (!deferredFallbackLogged) {
                deferredFallbackLogged = true;
                getLogger().debug("Using eager JSON record materialization because {}", reason);
            }
        }
    }

    private JsonTreeRowRecordReader createJsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null, tokenParserFactory);
    }

    private JsonTreeRowRecordReader createJsonTreeRowRecordReaderFromBytes(final byte[] input, final ComponentLog logger, final RecordSchema schema)
            throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(new ByteArrayInputStream(input), logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null, tokenParserFactory);
    }

    private StreamingJsonRowRecordReader createStreamingJsonRowRecordReader(final byte[] input, final ComponentLog logger, final RecordSchema schema)
            throws IOException {
        return new StreamingJsonRowRecordReader(input, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy,
                startingFieldName, schemaApplicationStrategy, tokenParserFactory);
    }

    private StreamingJsonRowRecordReader createStreamingJsonRowRecordReader(final InputStream input, final ComponentLog logger, final RecordSchema schema)
            throws IOException {
        return new StreamingJsonRowRecordReader(input, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy,
                startingFieldName, schemaApplicationStrategy, tokenParserFactory);
    }

    private StreamingJsonRowRecordReader createNonCapturingStreamingJsonRowRecordReader(
            final InputStream input, final ComponentLog logger, final RecordSchema schema) throws IOException {
        return new StreamingJsonRowRecordReader(input, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy,
                startingFieldName, schemaApplicationStrategy, tokenParserFactory, false);
    }

    private StreamingJsonRowRecordReader createValidatedStreamingJsonRowRecordReader(
            final byte[] input, final ComponentLog logger, final RecordSchema schema) throws IOException {
        return new StreamingJsonRowRecordReader(input, 0, input.length, logger, schema, dateFormat, timeFormat, timestampFormat,
                startingFieldStrategy, startingFieldName, schemaApplicationStrategy, tokenParserFactory);
    }

    private Map<String, String> normalizeVariables(final Map<String, String> variables) {
        return variables == null ? Map.of() : variables;
    }

    private ReplayableInputStream createReplayableInputStream(final InputStream input) {
        final int memoryThreshold = (int) Math.min(ReplayableInputStream.DEFAULT_MEMORY_THRESHOLD_BYTES, maxSchemaInferenceReplayBytes);
        return new ReplayableInputStream(input, memoryThreshold, maxSchemaInferenceReplayBytes);
    }

    private RecordReader retainReplayOwnership(final RecordReader reader, final ReplayableInputStream replayable) {
        return replayable == null || reader instanceof ValidatedInputStreamRecordReader
                ? reader : new ReplayOwningRecordReader(reader, replayable);
    }
}
