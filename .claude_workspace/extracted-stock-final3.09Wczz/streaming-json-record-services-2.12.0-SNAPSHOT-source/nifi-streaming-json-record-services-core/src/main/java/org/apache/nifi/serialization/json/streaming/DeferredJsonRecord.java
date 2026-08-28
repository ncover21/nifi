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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class DeferredJsonRecord implements Record {
    private final RecordSchema schema;
    private final boolean typeChecked;
    private final boolean dropUnknownFields;
    private final SchemaMutationSnapshot schemaMutationSnapshot;
    private final boolean serializedInputSemanticallyEquivalent;
    private Optional<SerializedForm> serializedForm;
    private Object materializer;
    private byte[] source;
    private final int offset;
    private final int length;

    private volatile Record delegate;

    DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                       final SerializedForm serializedForm, final RecordSupplier recordSupplier) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, recordSupplier, null, 0, 0);
    }

    DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                       final SerializedForm serializedForm, final RecordSupplier recordSupplier,
                       final SchemaMutationSnapshot schemaMutationSnapshot) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, recordSupplier, null, 0, 0, schemaMutationSnapshot, true);
    }

    DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                       final SerializedForm serializedForm, final RecordSupplier recordSupplier,
                       final SchemaMutationSnapshot schemaMutationSnapshot, final boolean serializedInputSemanticallyEquivalent) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, recordSupplier, null, 0, 0, schemaMutationSnapshot,
                serializedInputSemanticallyEquivalent);
    }

    DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                       final SerializedForm serializedForm, final RecordMaterializer materializer,
                       final byte[] source, final int offset, final int length) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, (Object) materializer, source, offset, length);
    }

    private DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                               final SerializedForm serializedForm, final Object materializer,
                               final byte[] source, final int offset, final int length) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, materializer, source, offset, length,
                SchemaMutationSnapshot.capture(schema), true);
    }

    DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                       final SerializedForm serializedForm, final RecordMaterializer materializer,
                       final byte[] source, final int offset, final int length, final SchemaMutationSnapshot schemaMutationSnapshot) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, (Object) materializer, source, offset, length, schemaMutationSnapshot);
    }

    private DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                               final SerializedForm serializedForm, final Object materializer,
                               final byte[] source, final int offset, final int length, final SchemaMutationSnapshot schemaMutationSnapshot) {
        this(schema, typeChecked, dropUnknownFields, serializedForm, materializer, source, offset, length, schemaMutationSnapshot, true);
    }

    private DeferredJsonRecord(final RecordSchema schema, final boolean typeChecked, final boolean dropUnknownFields,
                               final SerializedForm serializedForm, final Object materializer,
                               final byte[] source, final int offset, final int length, final SchemaMutationSnapshot schemaMutationSnapshot,
                               final boolean serializedInputSemanticallyEquivalent) {
        this.schema = schema;
        this.typeChecked = typeChecked;
        this.dropUnknownFields = dropUnknownFields;
        this.schemaMutationSnapshot = schemaMutationSnapshot;
        this.serializedInputSemanticallyEquivalent = serializedInputSemanticallyEquivalent;
        this.serializedForm = Optional.ofNullable(serializedForm);
        this.materializer = materializer;
        this.source = source;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public RecordSchema getSchema() {
        final Record record = delegate;
        return record == null ? schema : record.getSchema();
    }

    @Override
    public boolean isTypeChecked() {
        return typeChecked;
    }

    @Override
    public boolean isDropUnknownFields() {
        final Record record = delegate;
        return record == null ? dropUnknownFields : record.isDropUnknownFields();
    }

    @Override
    public Optional<SerializedForm> getSerializedForm() {
        final Record record = delegate;
        if (record != null) {
            return record.getSerializedForm();
        }
        final Optional<SerializedForm> pendingSerializedForm = serializedForm;
        if (pendingSerializedForm != null) {
            return pendingSerializedForm;
        }
        synchronized (this) {
            final Record materializedRecord = delegate;
            return materializedRecord == null ? Optional.empty() : materializedRecord.getSerializedForm();
        }
    }

    @Override
    public void incorporateSchema(final RecordSchema other) {
        getDelegate().incorporateSchema(other);
    }

    @Override
    public void incorporateInactiveFields() {
        getDelegate().incorporateInactiveFields();
    }

    @Override
    public Object[] getValues() {
        return getDelegate().getValues();
    }

    @Override
    public Object getValue(final String fieldName) {
        return getDelegate().getValue(fieldName);
    }

    @Override
    public Object getValue(final RecordField field) {
        return getDelegate().getValue(field);
    }

    @Override
    public String getAsString(final String fieldName) {
        return getDelegate().getAsString(fieldName);
    }

    @Override
    public String getAsString(final String fieldName, final String format) {
        return getDelegate().getAsString(fieldName, format);
    }

    @Override
    public String getAsString(final RecordField field, final String format) {
        return getDelegate().getAsString(field, format);
    }

    @Override
    public Long getAsLong(final String fieldName) {
        return getDelegate().getAsLong(fieldName);
    }

    @Override
    public Integer getAsInt(final String fieldName) {
        return getDelegate().getAsInt(fieldName);
    }

    @Override
    public Double getAsDouble(final String fieldName) {
        return getDelegate().getAsDouble(fieldName);
    }

    @Override
    public Float getAsFloat(final String fieldName) {
        return getDelegate().getAsFloat(fieldName);
    }

    @Override
    public Record getAsRecord(final String fieldName, final RecordSchema schema) {
        return getDelegate().getAsRecord(fieldName, schema);
    }

    @Override
    public Boolean getAsBoolean(final String fieldName) {
        return getDelegate().getAsBoolean(fieldName);
    }

    @Override
    public LocalDate getAsLocalDate(final String fieldName, final String format) {
        return getDelegate().getAsLocalDate(fieldName, format);
    }

    @Override
    public LocalDateTime getAsLocalDateTime(final String fieldName, final String format) {
        return getDelegate().getAsLocalDateTime(fieldName, format);
    }

    @Override
    public OffsetDateTime getAsOffsetDateTime(final String fieldName, final String format) {
        return getDelegate().getAsOffsetDateTime(fieldName, format);
    }

    @Override
    public Object[] getAsArray(final String fieldName) {
        return getDelegate().getAsArray(fieldName);
    }

    @Override
    public void setValue(final String fieldName, final Object value) {
        getDelegate().setValue(fieldName, value);
    }

    @Override
    public void setValue(final RecordField field, final Object value) {
        getDelegate().setValue(field, value);
    }

    @Override
    public void remove(final RecordField field) {
        getDelegate().remove(field);
    }

    @Override
    public boolean rename(final RecordField field, final String newName) {
        return getDelegate().rename(field, newName);
    }

    @Override
    public void regenerateSchema() {
        getDelegate().regenerateSchema();
    }

    @Override
    public void setArrayValue(final String fieldName, final int arrayIndex, final Object value) {
        getDelegate().setArrayValue(fieldName, arrayIndex, value);
    }

    @Override
    public void setMapValue(final String fieldName, final String mapKey, final Object value) {
        getDelegate().setMapValue(fieldName, mapKey, value);
    }

    @Override
    public Set<String> getRawFieldNames() {
        return getDelegate().getRawFieldNames();
    }

    @Override
    public Map<String, Object> toMap() {
        return getDelegate().toMap();
    }

    @Override
    public int hashCode() {
        return getDelegate().hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }
        return other instanceof final DeferredJsonRecord deferred && getDelegate().equals(deferred.getDelegate());
    }

    @Override
    public String toString() {
        return getDelegate().toString();
    }

    private Record getDelegate() {
        Record record = delegate;
        if (record == null) {
            synchronized (this) {
                record = delegate;
                if (record == null) {
                    try {
                        final Object pendingMaterializer = materializer;
                        final byte[] pendingSource = source;
                        record = pendingMaterializer instanceof final RecordSupplier supplier
                                ? supplier.get()
                                : ((RecordMaterializer) pendingMaterializer).materialize(
                                        pendingSource, offset, length, typeChecked, dropUnknownFields);
                    } catch (final Exception e) {
                        throw new IllegalStateException("Failed to materialize validated JSON record", e);
                    }
                    serializedForm = null;
                    materializer = null;
                    source = null;
                    delegate = record;
                }
            }
        }
        return record;
    }

    boolean isMaterialized() {
        return delegate != null;
    }

    boolean hasPendingState() {
        return serializedForm != null || materializer != null || source != null;
    }

    boolean isSerializedSchemaUnmodified() {
        return delegate == null && schemaMutationSnapshot.isUnmodified();
    }

    boolean isSerializedInputSemanticallyEquivalent() {
        return serializedInputSemanticallyEquivalent;
    }

    @FunctionalInterface
    interface RecordSupplier {
        Record get() throws Exception;
    }

    @FunctionalInterface
    interface RecordMaterializer {
        Record materialize(byte[] source, int offset, int length, boolean coerceTypes, boolean dropUnknownFields) throws Exception;
    }
}
