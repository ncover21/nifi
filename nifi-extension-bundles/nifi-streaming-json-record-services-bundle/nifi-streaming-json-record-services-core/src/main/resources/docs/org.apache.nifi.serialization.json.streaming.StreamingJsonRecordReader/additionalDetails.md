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

# Streaming JSON Record Reader

The Streaming JSON Record Reader converts JSON objects into normal NiFi Records without constructing a complete JSON tree for supported inputs. It can be selected by any Processor that accepts a `RecordReaderFactory`; it is not limited to Kafka.

The service is opt-in. Installing or enabling it does not change existing `JsonTreeReader` services or flows.

## JSON document boundaries

The input can contain:

- one JSON object, producing one Record;
- a root JSON array, producing one Record for each object in the array; or
- multiple JSON objects separated by whitespace, including one-object-per-line JSON.

For example, both inputs produce two Records with fields `id` and `name`:

```json
[
  {"id": 1, "name": "one"},
  {"id": 2, "name": "two"}
]
```

```text
{"id":1,"name":"one"}
{"id":2,"name":"two"}
```

With an explicit schema, the standard `nextRecord()` behavior omits JSON fields absent from the schema. Callers using the lower-level `nextRecord(coerceTypes, dropUnknownFields)` method control unknown-field removal with `dropUnknownFields`. Schema fields absent from the JSON use their default value when one is defined and otherwise have a null value.

## Schemas and type conversion

Schema Access Strategy determines whether the service infers a schema, reads it from a registry or schema reference, or evaluates Schema Text. Values are converted according to the selected Record schema. Common conversions include compatible numeric widening, strings to configured numeric or temporal types, numeric epoch milliseconds to temporal types, and temporal values to their corresponding string or long representations. A value that cannot be converted to the configured type causes record processing to fail.

When `Schema Access Strategy` is `Infer Schema`, inference follows these rules:

- inferred fields are nullable;
- compatible numeric observations widen to a common numeric type;
- incompatible observations form a choice type unless one type safely encompasses the other;
- configured Date, Time, and Timestamp formats are considered before a string is inferred as a string;
- JSON objects infer as Record types rather than Map types; and
- a field whose observed values are all null infers as a nullable string.

## Full-input inference and schema caching

`Infer Schema` scans the complete input before returning the first Record so that one schema covers every record. Supported paths avoid constructing a complete JSON tree, but they do not remove this full-input inference pass or its first-record latency.

A configured Schema Inference Cache can avoid inference when the FlowFile contains a `schema.cache.identifier` that resolves to a cached schema. A cache miss still requires inference. An explicit, referenced, or successfully cached schema avoids the inference prescan and reads records sequentially.

Normal NiFi ProcessSession content can be rewound by the host without a replay file. Other InputStreams are captured for the second pass: the first 1 MiB is kept in memory and additional content spills to a delete-on-close file in the JVM temporary directory. `Maximum Schema Inference Replay Size` is a hard bound for each concurrently open reader and defaults to 1 GiB. Exceeding it fails reader creation. Size the aggregate temporary-storage budget for concurrent tasks, lower the bound for untrusted inputs, or use an explicit or cached schema for larger content.

## Deferred and eager materialization

`Record Materialization Strategy` applies to inferred-schema reading:

| Strategy | Behavior | Recommended use |
| --- | --- | --- |
| Prefer Deferred | Preserves validated record bytes and defers conversion of the entire Record until the first field or value access. Automatically falls back to eager typed or compatibility decoding when required. | Pass-through and grouping paths that do not inspect fields, and compatible JSON writing. |
| Eager | Converts each field while the Record is read. Conversion failures occur during sequential reading. | Queries, updates, validation, or flows that inspect most fields. |

Deferred InputStream processing is eligible for standard strict UTF-8 JSON at the root, without a schema cache, when validated metadata remains within these optimization bounds:

- at most 1,024 logical records;
- at most 16 MiB for one captured record; and
- at most 64 MiB of captured record bytes for one reader.

These are optimization thresholds, not input-rejection limits. When they are exceeded, the service continues with eager typed decoding. Eager inferred-schema InputStream processing does not retain reusable raw record bytes.

Deferred Records can outlive reader advancement and closing. Conversion can still fail when a field is first accessed. The first field or value access materializes the complete Record, not one field in isolation. A downstream operation that reads or modifies fields therefore disables serialized reuse, after which the writer uses normal typed serialization.

## Starting fields

`Starting Field Strategy = Nested Field` begins Record processing at the named nested object or array. For this input:

```json
{
  "source": "example",
  "events": [
    {"id": 1},
    {"id": 2}
  ]
}
```

setting `Starting Field Name = events` produces two Records containing `id`.

For an explicit schema, `Schema Application Strategy = Selected Part` means the schema describes the selected object or array elements. `Whole JSON` means the schema describes the complete document before the nested field is selected. Nested starting fields are not eligible for deferred serialized reuse, but continue through typed token streaming when the selected schema is supported. Only schemas unsupported by the streaming row converter require the bundle-owned tree compatibility reader.

## Parsing, encoding, and fallback

Standard parsing provides the strict UTF-8 byte boundaries required for serialized JSON reuse. Lenient parsing accepts the syntax enabled by the Parsing Strategy but is not eligible for deferred raw reuse. UTF-8 BOM and UTF-16 or UTF-32 input are decoded correctly but are not exposed as reusable UTF-8 serialized forms. These cases continue through typed token streaming when their schema is supported.

Schema caching, nested selection, lenient syntax, non-UTF-8 encoding, or metadata limits can make the deferred/raw path ineligible and select eager or non-capturing typed token decoding. A schema unsupported by the streaming row converter selects the tree compatibility reader. Every fallback changes the execution strategy, not Record values, relationships, or the public reader contract.

## Stock and enhanced lanes

Both mutually exclusive artifacts expose this same Controller Service name and behavior:

| Lane | Host requirement | Input integration |
| --- | --- | --- |
| Stock | Exact supported unmodified NiFi release | Standard `RecordReaderFactory` InputStream contract. |
| Enhanced | Exact NiFi build containing the optional byte-array and Record lifetime APIs | Standard InputStream contract plus direct immutable `byte[]` input for capable message-backed processors. |

Kafka can use the enhanced direct-byte path only when it references this reader directly on a matching host. Selecting it through `ReaderLookup` remains correct but exposes the standard InputStream contract. Other Record processors work through the normal reader interface in either lane.

## Resource limits

- `Maximum Schema Inference Fields` defaults to 10,000 distinct fields across the complete inferred schema, including nested records and array elements.
- `Maximum JSON Nesting Depth` defaults to 1,000 object and array levels.
- `Max String Length` bounds individual JSON strings.
- `Maximum Schema Inference Replay Size` bounds captured generic InputStream content per open reader.

Lower these limits for untrusted inputs. Use an explicit schema for legitimately wider or larger records when possible.
