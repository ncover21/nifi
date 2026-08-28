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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Create New FlowFile strategy: groups by write schema, topic, partition, and grouping attributes,
 * streaming records into an open writer per group.
 */
public class CreateNewFlowFileGrouping implements RecordGroupingStrategy {

    private final RecordSetWriterFactory writerFactory;
    private final ComponentLog logger;
    private final String brokerUri;
    private final boolean commitOffsets;
    private final Map<RecordGroupCriteria, RecordGroup> recordGroups = new HashMap<>();

    public CreateNewFlowFileGrouping(
            final RecordSetWriterFactory writerFactory,
            final ComponentLog logger,
            final String brokerUri,
            final boolean commitOffsets) {
        this.writerFactory = writerFactory;
        this.logger = logger;
        this.brokerUri = brokerUri;
        this.commitOffsets = commitOffsets;
    }

    @Override
    public void addRecord(
            final ProcessSession session,
            final ByteRecord consumerRecord,
            final Record recordToWrite,
            final RecordSchema writeSchema,
            final Map<String, String> attributes,
            final Map<String, String> groupingAttributes) throws IOException, SchemaNotFoundException {
        final String topic = consumerRecord.getTopic();
        final int partition = consumerRecord.getPartition();

        final RecordGroupCriteria criteria = new RecordGroupCriteria(writeSchema, groupingAttributes, topic, partition);
        RecordGroup group = recordGroups.get(criteria);
        if (group == null) {
            FlowFile ff = session.create();
            ff = session.putAllAttributes(ff, Map.of(
                    KafkaFlowFileAttribute.KAFKA_TOPIC, topic,
                    KafkaFlowFileAttribute.KAFKA_PARTITION, String.valueOf(partition)));

            final OutputStream out = session.write(ff);
            RecordSetWriter writer = null;
            try {
                writer = writerFactory.createWriter(logger, writeSchema, out, attributes);
                writer.beginRecordSet();
            } catch (final IOException | SchemaNotFoundException | RuntimeException | Error ex) {
                closeAfterInitializationFailure(writer, out, ex);
                try {
                    session.remove(ff);
                } catch (final RuntimeException | Error removalFailure) {
                    if (removalFailure != ex) {
                        removalFailure.addSuppressed(ex);
                    }
                    throw removalFailure;
                }
                throw ex;
            }

            final long offset = consumerRecord.getOffset();
            final AtomicLong maxOffset = new AtomicLong(offset);
            final AtomicLong minOffset = new AtomicLong(offset);
            final AtomicLong minTimestamp = new AtomicLong(consumerRecord.getTimestamp());
            group = new RecordGroup(ff, writer, maxOffset, minOffset, minTimestamp);
            recordGroups.put(criteria, group);
        } else {
            final long recordOffset = consumerRecord.getOffset();
            final AtomicLong maxOffset = group.maxOffset();
            if (recordOffset > maxOffset.get()) {
                maxOffset.set(recordOffset);
            }

            final AtomicLong minOffset = group.minOffset();
            if (recordOffset < minOffset.get()) {
                minOffset.set(recordOffset);
            }

            final long recordTimestamp = consumerRecord.getTimestamp();
            final AtomicLong minTimestamp = group.minTimestamp();
            if (recordTimestamp < minTimestamp.get()) {
                minTimestamp.set(recordTimestamp);
            }
        }

        group.writer().write(recordToWrite);
    }

    private static void closeAfterInitializationFailure(final RecordSetWriter writer, final OutputStream out, final Throwable failure) {
        if (writer != null) {
            try {
                writer.close();
            } catch (final Exception | Error closeFailure) {
                if (failure != closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        try {
            out.close();
        } catch (final Exception | Error closeFailure) {
            if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    @Override
    public void finishAllGroups(final ProcessSession session) {
        final Iterator<Map.Entry<RecordGroupCriteria, RecordGroup>> groupIterator = recordGroups.entrySet().iterator();
        while (groupIterator.hasNext()) {
            final Map.Entry<RecordGroupCriteria, RecordGroup> e = groupIterator.next();
            final RecordGroupCriteria criteria = e.getKey();
            final RecordGroup group = e.getValue();

            final Map<String, String> resultAttrs = new HashMap<>();
            final int recordCount;
            try (final RecordSetWriter writer = group.writer()) {
                final WriteResult writeResult = writer.finishRecordSet();
                resultAttrs.putAll(writeResult.getAttributes());
                resultAttrs.put("record.count", String.valueOf(writeResult.getRecordCount()));
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_COUNT, String.valueOf(writeResult.getRecordCount()));
                resultAttrs.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

                final long maxOffset = group.maxOffset().get();
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, Long.toString(maxOffset));

                final long minOffset = group.minOffset().get();
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_OFFSET, Long.toString(minOffset));

                final long minTimestamp = group.minTimestamp().get();
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_TIMESTAMP, Long.toString(minTimestamp));

                resultAttrs.putAll(criteria.groupingAttributes());
                resultAttrs.put(KafkaFlowFileAttribute.KAFKA_CONSUMER_OFFSETS_COMMITTED, String.valueOf(commitOffsets));
                recordCount = writeResult.getRecordCount();
            } catch (final Exception ex) {
                groupIterator.remove();
                throw new ProcessException("Failed to write Kafka records to FlowFile", ex);
            }
            groupIterator.remove();

            FlowFile ff = group.flowFile();
            ff = session.putAllAttributes(ff, resultAttrs);

            session.getProvenanceReporter().receive(ff, brokerUri + "/" + criteria.topic());
            session.adjustCounter("Records Received from " + criteria.topic(), recordCount, false);
            session.transfer(ff, ConsumeKafka.SUCCESS);
        }
    }

    @Override
    public void abortAllGroups() {
        Throwable closeFailure = null;
        for (final RecordGroup group : recordGroups.values()) {
            try {
                group.writer().close();
            } catch (final Exception | Error e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else if (closeFailure != e) {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        recordGroups.clear();
        if (closeFailure instanceof final Error error) {
            throw error;
        }
        if (closeFailure instanceof final RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (closeFailure != null) {
            throw new ProcessException("Failed to close Kafka record writer", closeFailure);
        }
    }

    private record RecordGroupCriteria(RecordSchema schema, Map<String, String> groupingAttributes, String topic, int partition) {
    }

    private record RecordGroup(FlowFile flowFile, RecordSetWriter writer, AtomicLong maxOffset, AtomicLong minOffset, AtomicLong minTimestamp) {
    }
}
