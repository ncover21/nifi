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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.common.HeaderValueConverter;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.ByteArrayRecordReaderFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Shared reader loop, parse-failure handling, and record conversion for record-stream converters.
 * FlowFile grouping and finalization are delegated to a {@link RecordGroupingStrategy}.
 */
public abstract class AbstractRecordStreamKafkaMessageConverter implements KafkaMessageConverter {
    private static final int MAX_CACHED_WRITE_SCHEMAS = 64;

    protected final RecordReaderFactory readerFactory;
    protected final RecordSetWriterFactory writerFactory;
    protected final HeaderValueConverter headerValueConverter;
    protected final Pattern headerNamePattern;
    protected final KeyEncoding keyEncoding;
    protected final boolean commitOffsets;
    protected final OffsetTracker offsetTracker;
    protected final ComponentLog logger;
    protected final String brokerUri;
    private final RecordGroupingStrategy recordGroupingStrategy;

    protected AbstractRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final HeaderValueConverter headerValueConverter,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri,
            final RecordGroupingStrategy recordGroupingStrategy) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.headerValueConverter = headerValueConverter;
        this.headerNamePattern = headerNamePattern;
        this.keyEncoding = keyEncoding;
        this.commitOffsets = commitOffsets;
        this.offsetTracker = offsetTracker;
        this.logger = logger;
        this.brokerUri = brokerUri;
        this.recordGroupingStrategy = recordGroupingStrategy;
    }

    @Override
    public void toFlowFiles(final ProcessSession session, final Iterator<ByteRecord> consumerRecords) {
        try {
            while (consumerRecords.hasNext()) {
                final ByteRecord consumerRecord = consumerRecords.next();
                final byte[] value = consumerRecord.getValue();

                final Map<String, String> attributes = KafkaUtils.toAttributes(
                        consumerRecord, keyEncoding, headerNamePattern, headerValueConverter, commitOffsets);

                final Map<String, String> groupingAttributes = extractHeaders(consumerRecord);

                try {
                    final RecordReader reader = createRecordReader(attributes, value);
                    addRecordsAsRead(session, consumerRecord, attributes, groupingAttributes, reader);
                } catch (final ProcessException e) {
                    throw e;
                } catch (final MalformedRecordException | IOException | SchemaNotFoundException e) {
                    logger.debug("Reader or Writer failed to process Kafka Record with Topic [{}] Partition [{}] Offset [{}]",
                            consumerRecord.getTopic(), consumerRecord.getPartition(), consumerRecord.getOffset(), e);
                    handleParseFailure(session, consumerRecord, attributes, value);
                    offsetTracker.update(consumerRecord);
                    continue;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to process Kafka message", e);
                }

                offsetTracker.update(consumerRecord);
            }

            recordGroupingStrategy.finishAllGroups(session);
        } catch (final RuntimeException | Error failure) {
            try {
                recordGroupingStrategy.abortAllGroups();
            } catch (final RuntimeException | Error abortFailure) {
                if (failure != abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
            }
            throw failure;
        }
    }

    private RecordReader createRecordReader(final Map<String, String> attributes, final byte[] value)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (readerFactory instanceof final ByteArrayRecordReaderFactory byteArrayReaderFactory) {
            return byteArrayReaderFactory.createRecordReaderFromBytes(attributes, value, logger);
        }
        return readerFactory.createRecordReader(attributes, new ByteArrayInputStream(value), value.length, logger);
    }

    private void addRecordsAsRead(final ProcessSession session, final ByteRecord consumerRecord, final Map<String, String> attributes,
                                  final Map<String, String> groupingAttributes, final RecordReader reader)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        try (reader) {
            final boolean retainStreamingRecords = reader.getRecordHandlingMode() == RecordReader.RecordHandlingMode.STREAMING
                    && recordGroupingStrategy.isRecordRetentionRequired();
            Record record = reader.nextRecord();
            if (record == null) {
                return;
            }

            final RecordSchema firstReadSchema = record.getSchema();
            final RecordSchema firstWriteSchema = resolveWriteSchema(firstReadSchema, consumerRecord, attributes);
            addToGroup(session, consumerRecord, prepareRecord(consumerRecord, record, attributes, retainStreamingRecords), firstWriteSchema,
                    attributes, groupingAttributes);

            final WriteSchemaResolver schemaResolver = new WriteSchemaResolver(
                    firstReadSchema, firstWriteSchema, consumerRecord, attributes);
            while ((record = reader.nextRecord()) != null) {
                final RecordSchema writeSchema = schemaResolver.resolve(record.getSchema());
                addToGroup(session, consumerRecord, prepareRecord(consumerRecord, record, attributes, retainStreamingRecords), writeSchema,
                        attributes, groupingAttributes);
            }
        }
    }

    private Record prepareRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes,
                                 final boolean retainStreamingRecords) throws IOException {
        final Record converted = convertRecord(consumerRecord, record, attributes);
        return retainStreamingRecords ? retainRecord(converted) : converted;
    }

    private Record retainRecord(final Record record) {
        final Map<String, Object> retainedValues = new LinkedHashMap<>();
        record.toMap().forEach((fieldName, value) -> retainedValues.put(fieldName, retainValue(value)));
        return new MapRecord(record.getSchema(), retainedValues, record.isTypeChecked(), record.isDropUnknownFields());
    }

    private Object retainValue(final Object value) {
        if (value instanceof final Record record) {
            return retainRecord(record);
        }
        if (value instanceof final Map<?, ?> map) {
            final Map<Object, Object> retained = new LinkedHashMap<>(map.size());
            map.forEach((key, mapValue) -> retained.put(key, retainValue(mapValue)));
            return retained;
        }
        if (value instanceof final Set<?> set) {
            final Set<Object> retained = new LinkedHashSet<>(set.size());
            set.forEach(element -> retained.add(retainValue(element)));
            return retained;
        }
        if (value instanceof final Collection<?> collection) {
            final List<Object> retained = new ArrayList<>(collection.size());
            collection.forEach(element -> retained.add(retainValue(element)));
            return retained;
        }
        if (value != null && value.getClass().isArray()) {
            final int length = Array.getLength(value);
            final Class<?> componentType = value.getClass().getComponentType();
            if (componentType.isPrimitive()) {
                final Object retained = Array.newInstance(componentType, length);
                System.arraycopy(value, 0, retained, 0, length);
                return retained;
            }
            final Object[] source = (Object[]) value;
            final Object[] retained = (Object[]) Array.newInstance(componentType, length);
            for (int i = 0; i < length; i++) {
                final Object retainedValue = retainValue(source[i]);
                if (retainedValue != null && !componentType.isInstance(retainedValue)) {
                    final Object[] compatible = new Object[length];
                    System.arraycopy(retained, 0, compatible, 0, i);
                    compatible[i] = retainedValue;
                    for (int j = i + 1; j < length; j++) {
                        compatible[j] = retainValue(source[j]);
                    }
                    return compatible;
                }
                retained[i] = retainedValue;
            }
            return retained;
        }
        if (value instanceof final Timestamp timestamp) {
            final Timestamp retained = new Timestamp(timestamp.getTime());
            retained.setNanos(timestamp.getNanos());
            return retained;
        }
        if (value instanceof final java.sql.Date date) {
            return new java.sql.Date(date.getTime());
        }
        if (value instanceof final java.sql.Time time) {
            return new java.sql.Time(time.getTime());
        }
        if (value instanceof final Date date) {
            return new Date(date.getTime());
        }
        return value;
    }

    private final class WriteSchemaResolver {
        private final RecordSchema firstReadSchema;
        private final RecordSchema firstWriteSchema;
        private final ByteRecord consumerRecord;
        private final Map<String, String> attributes;
        private Map<RecordSchema, RecordSchema> cachedWriteSchemas;
        private boolean cacheWriteSchemas = true;

        private WriteSchemaResolver(final RecordSchema firstReadSchema, final RecordSchema firstWriteSchema,
                                    final ByteRecord consumerRecord, final Map<String, String> attributes) {
            this.firstReadSchema = firstReadSchema;
            this.firstWriteSchema = firstWriteSchema;
            this.consumerRecord = consumerRecord;
            this.attributes = attributes;
        }

        private RecordSchema resolve(final RecordSchema readSchema) throws IOException {
            if (firstReadSchema == readSchema) {
                return firstWriteSchema;
            }
            if (!cacheWriteSchemas) {
                return resolveWriteSchema(readSchema, consumerRecord, attributes);
            }
            if (cachedWriteSchemas == null) {
                cachedWriteSchemas = new IdentityHashMap<>(4);
            }

            final RecordSchema cachedSchema = cachedWriteSchemas.get(readSchema);
            if (cachedSchema != null || cachedWriteSchemas.containsKey(readSchema)) {
                return cachedSchema;
            }

            final RecordSchema writeSchema = resolveWriteSchema(readSchema, consumerRecord, attributes);
            cachedWriteSchemas.put(readSchema, writeSchema);
            if (cachedWriteSchemas.size() == MAX_CACHED_WRITE_SCHEMAS - 1) {
                cachedWriteSchemas = null;
                cacheWriteSchemas = false;
            }
            return writeSchema;
        }
    }

    private RecordSchema resolveWriteSchema(final RecordSchema readSchema, final ByteRecord consumerRecord,
                                            final Map<String, String> attributes) throws IOException {
        return getWriteSchema(readSchema, consumerRecord, attributes);
    }

    private void addToGroup(final ProcessSession session, final ByteRecord consumerRecord, final Record record,
                            final RecordSchema writeSchema, final Map<String, String> attributes,
                            final Map<String, String> groupingAttributes) throws IOException, SchemaNotFoundException {
        recordGroupingStrategy.addRecord(session, consumerRecord, record, writeSchema, attributes, groupingAttributes);
    }

    protected void handleParseFailure(final ProcessSession session, final ByteRecord consumerRecord, final Map<String, String> attributes, final byte[] value) {
        FlowFile ff = session.create();
        ff = session.putAllAttributes(ff, attributes);
        ff = session.write(ff, out -> out.write(value));
        session.transfer(ff, ConsumeKafka.PARSE_FAILURE);
        session.adjustCounter("Records Received from " + consumerRecord.getTopic(), 1, false);
    }

    /**
     * By default we do *not* promote any headers to FlowFile attributes.
     **/
    protected Map<String, String> extractHeaders(final ByteRecord consumerRecord) {
        return Map.of();
    }

    protected abstract RecordSchema getWriteSchema(RecordSchema inputSchema, ByteRecord consumerRecord, Map<String, String> attributes) throws IOException;

    protected abstract Record convertRecord(ByteRecord consumerRecord, Record record, Map<String, String> attributes) throws IOException;
}
