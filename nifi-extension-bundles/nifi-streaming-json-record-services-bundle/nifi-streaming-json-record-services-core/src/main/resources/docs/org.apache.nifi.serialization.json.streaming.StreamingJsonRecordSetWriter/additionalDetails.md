<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Streaming JSON Record Set Writer

The Streaming JSON Record Set Writer writes normal NiFi Records as a JSON array or as one JSON object per line. It accepts Records from any Record Reader and is not limited to Kafka. Pairing it with `StreamingJsonRecordReader` enables serialized JSON reuse, but is not required for correctness.

## Output grouping

`Output Grouping = Array` wraps every result in a JSON array, including a one-record result:

```json
[{"id":1}]
```

`One Line Per Object` writes newline-delimited JSON without an outer array:

```text
{"id":1}
{"id":2}
```

Pretty Print JSON cannot be enabled with One Line Per Object.

## Serialized JSON input handling

`Serialized JSON Input Handling` controls whether the writer can emit an eligible Record's original JSON representation instead of converting and writing every field.

| Setting | Behavior |
| --- | --- |
| Enabled | Reuses eligible validated JSON verbatim. This is the default and is optimized for pass-through and grouping. Writer-side Date, Time, Timestamp, and null-suppression settings might not be applied to a reused record. |
| Disabled | Materializes and serializes every Record from typed values. Use this when writer formatting and suppression properties must be honored uniformly. |

Only an unmaterialized deferred Record created by this bundle's validated reader can use the raw path. The writer also requires all of the following:

- an `application/json` serialized form containing valid reusable UTF-8 JSON;
- no direct or nested Record mutation and no source- or writer-schema mutation;
- `Timestamp Representation = Automatic`;
- matching pretty-print state;
- no scientific notation in the input when scientific notation is disabled; and
- an equal schema, or a narrowly compatible merged schema that does not change existing field names or types.

Reading or changing a deferred field materializes the Record and selects typed serialization. Records from other readers, unsupported serialized forms, recursive or incompatible schemas, numeric widening, aliases that change field identity, defaults, and nested structural evolution also use typed serialization automatically.

Raw reuse is a performance strategy, not a different public output mode. When an eligibility check fails, the writer falls back to typed output without changing the Processor relationship or Record count.

### Property interactions

Raw reuse preserves the eligible input object exactly except for framing and supported top-level missing-field injection. It therefore preserves existing nulls and temporal representations even when writer properties request something else.

For example, with `Suppress Null Values = Always Suppress`, an eligible unmaterialized input can remain:

```json
{"id":1,"optional":null}
```

Set `Serialized JSON Input Handling = Disabled` to guarantee:

```json
{"id":1}
```

Similarly, disabling serialized input handling guarantees that configured Date, Time, and Timestamp formats are applied. Selecting a non-Automatic Timestamp Representation also forces typed serialization.

## Merged schemas

Raw reuse supports exact schemas and one narrow merged-schema case: existing fields must retain exactly the same types, while additional writer fields must be nullable and have no default. With `Suppress Null Values = Never Suppress`, missing top-level fields are appended as null when compact output is used:

```json
{"id":1,"newField":null}
```

Other schema changes use typed serialization. In particular, numeric widening and nested structural evolution are not considered byte-compatible even when the Record schemas can otherwise be merged.

## Timestamp representation

`Timestamp Representation` affects Timestamp logical fields; Date and Time fields continue to use their corresponding format properties.

| Value | Output |
| --- | --- |
| Automatic | Uses Timestamp Format when configured; otherwise epoch milliseconds. Allows eligible serialized JSON reuse. |
| Formatted String | Uses Timestamp Format and requires that property to be configured. |
| Epoch Milliseconds | Writes an integer number of milliseconds since the Unix epoch. |
| Epoch Seconds | Writes decimal seconds since the Unix epoch with millisecond precision. |

The three explicit representations use typed serialization so their output is applied consistently.

## Compression and schema metadata

The writer supports no compression, GZIP, BZIP2, ZSTD, XZ-LZMA2, Snappy, and Snappy Framed. Compression wraps both raw and typed JSON output and does not change eligibility rules. Compression Level applies only to GZIP.

Schema Write Strategy, schema attributes, MIME type, record counts, framing, flushing, and closing are the same for raw and typed records.

## Processor compatibility

The writer implements the standard `RecordSetWriterFactory` contract and works with processors such as `ConvertRecord`, `QueryRecord`, `UpdateRecord`, `PartitionRecord`, `MergeRecord`, `SplitRecord`, `ValidateRecord`, and Kafka Record processors. A transformed or materialized Record uses typed JSON automatically, so pairing with the streaming reader is optional.
