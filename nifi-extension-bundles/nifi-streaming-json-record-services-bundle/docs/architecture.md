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

# Streaming JSON Record Services architecture

This document describes the implementation and maintenance contracts of the independent Streaming JSON Record Services bundle. Operator-facing behavior remains in the Controller Service Usage pages.

## Invariants

- `StreamingJsonRecordReader` and `StreamingJsonRecordSetWriter` are opt-in services with stable public class names.
- Existing `JsonTreeReader` and `JsonRecordSetWriter` services are not replaced or modified by this bundle.
- Every supported path produces normal NiFi Records and implements the standard Record Reader or Writer factory contract.
- Optimization eligibility can change execution strategy but cannot change Record values, Processor relationships, or lifecycle ownership.
- Stock and enhanced NARs have the same runtime NAR identity and must never be installed together.
- Runtime artifacts do not depend on the standard JSON implementation NAR or Kafka processor classes.

## Modules

| Module | Responsibility |
| --- | --- |
| `nifi-streaming-json-record-services-core` | Host-neutral properties, schema selection, inference, token decoding, deferred Records, replay, raw/typed writing, and writer service. |
| `nifi-streaming-json-record-services-stock` | Reader adapter compiled against an unmodified supported NiFi host. |
| `nifi-streaming-json-record-services-enhanced` | Reader adapter implementing the host-owned direct-byte capability. |
| `nifi-streaming-json-record-services-stock-nar` | Stock-host NAR. |
| `nifi-streaming-json-record-services-nar` | Enhanced matching-host NAR. |
| `nifi-streaming-json-record-services-processor-tests` | Representative compatibility tests through public Record interfaces. |
| `nifi-streaming-json-record-services-distribution` | Lane-specific binary archives, source archive, compatibility metadata, checksums, and SBOM. |

The core contains no reference to `ByteArrayRecordReaderFactory`. Only the enhanced adapter links that host-owned API. This separation lets the same core source target a released stock NiFi version and a matching enhanced host.

## Reader execution paths

| Schema and input | Primary path | Ownership |
| --- | --- | --- |
| Enhanced immutable `byte[]`, direct inference, deferred | Full token inference with validated record offsets; Records retain slices of the caller-owned array until materialization. | Caller must not mutate the array while Records are live. |
| InputStream, direct inference, deferred | Full token inference, rewind, then record-bounded copies using validated offsets. | Each returned Record owns its captured bytes. |
| Direct inference, eager | Full token inference followed by sequential typed token decoding. | Typed Record values own their data. |
| Explicit, referenced, or cached schema | Sequential streaming conversion when supported. | Typed Record values and optional bounded serialized form. |
| Deferred/raw ineligible, streaming schema supported | Eager or non-capturing typed token streaming. | Normal typed Record ownership. |
| Streaming row schema unsupported | Bundle-owned tree compatibility reader. | Normal typed Record ownership. |

Inference and reading share `StreamingJsonSchemaInference`, `JsonParserRecordSource`, and the streaming row decoder. Unsupported cases do not load implementation classes from the standard JSON NAR.

### Full-input inference

An inferred schema must describe all logical records, so inference consumes the complete input before the first Record is returned. The reader then resets or replays the input for Record production.

`RewindableInputStreamAccess` recognizes exact framework streams that can safely reset across the complete content: `ContentClaimInputStream`, `ByteArrayInputStream`, `ReplayableInputStream`, and a framework `TaskTerminationInputStream` whose delegate is one of those rewindable types. Other streams require `ReplayableInputStream`, even if they advertise mark support, because a finite mark read limit cannot prove whole-input rewindability.

Custom integrations must pass their ordinary InputStream. They must not construct NiFi's internal `TaskTerminationInputStream` around a finite-mark delegate to obtain the framework fast path.

### Replay ownership

Replay capture retains 1 MiB in memory and then spills to a file created by `Files.createTempFile`, which uses the JVM temporary directory. The configured maximum is enforced while capturing, not from the Record Reader API's advisory input-length argument.

The returned RecordReader owns the replay resource. Reader construction closes it on checked, runtime, and `Error` failures. `ReplayOwningRecordReader` closes the replay after the delegated reader, preserves the primary failure, and avoids self-suppression. Processors must close RecordReaders according to the standard API contract.

## Deferred Record contract

`DeferredJsonRecord` carries a schema, validated serialized form, schema mutation snapshot, and a materializer. The first field or value access materializes the complete Record exactly once. Direct or nested mutations invalidate semantic equivalence with the original serialized form.

The reader reports `RecordHandlingMode.RETAINABLE` for deferred Records because they remain valid after reader advancement and closing, but a later field access can still fail conversion. It does not claim complete materialization validation.

InputStream deferral uses owned per-record byte arrays. Enhanced direct-byte deferral can share the immutable source array. No returned Record references a moving parser buffer or replay ring.

## Bounds and fallback

| Bound | Default or fixed value | Result when exceeded |
| --- | ---: | --- |
| Schema inference replay | 1 GiB configurable per reader | Reader creation fails. |
| In-memory replay threshold | 1 MiB | Capture spills to a temporary file. |
| Inferred fields | 10,000 configurable | Inference fails. |
| JSON nesting depth | 1,000 configurable | Parsing fails. |
| Deferred metadata records | 1,024 | Reader falls back to eager streaming. |
| Captured deferred record | 16 MiB | Reader falls back to eager streaming. |
| Captured deferred bytes | 64 MiB per reader | Reader falls back to eager streaming. |

The replay, inference-field, nesting, and string limits protect correctness and resources and therefore fail explicitly. Deferred metadata and capture limits only bound an optimization and therefore select eager typed processing.

## Writer execution paths

`StreamingJsonRecordSetWriter` has one framing, compression, schema metadata, flush, finish, and close lifecycle. Individual Records choose raw or typed field writing.

Raw serialized reuse requires an unmaterialized `DeferredJsonRecord` produced by this bundle, an unchanged source and writer schema state, `application/json`, reusable UTF-8 bytes or string content, matching pretty-print state, compatible scientific-notation policy, and `Timestamp Representation = Automatic`.

Schema compatibility is intentionally narrow:

- equal schemas are compatible;
- additional top-level writer fields are compatible only when nullable and without defaults;
- existing fields must retain the same canonical names and exact data types; and
- recursive schemas, numeric widening, aliases that change identity, defaults, and nested evolution use typed writing.

When compact output and `Never Suppress` are configured, compatible missing top-level fields can be appended as null without materializing the source object. Pretty output with missing-field injection uses typed writing.

Raw reuse deliberately preserves existing source JSON. It does not rewrite existing nulls or temporal values according to writer formatting properties. `Serialized JSON Input Handling = Disabled` guarantees typed normalization. Any non-Automatic Timestamp Representation also forces typed writing.

## Processor and Kafka integration

Both lanes work through the standard Record interfaces used by generic processors. No processor requires a JSON-specific branch.

The enhanced host owns `ByteArrayRecordReaderFactory` because Kafka and the Controller Service load through different NAR classloaders. Kafka performs format-neutral capability dispatch: a capable reader receives the immutable message bytes, and every other reader receives the existing InputStream path. `ReaderLookup` returns the standard interface and therefore remains correct without exposing direct-byte dispatch.

Kafka grouping uses `RecordHandlingMode` to determine whether a Record must be copied before retention. Group creation and abort paths close every partially initialized writer and output, clear group state, preserve complete failure provenance through primary or suppressed exceptions, and guard against self-suppression. A distinct FlowFile-removal failure becomes primary because cleanup failure can leave externally visible session state.

## Verification contract

Every behavioral change requires:

- focused core and adapter tests;
- stock and enhanced processor compatibility profiles;
- NAR provider and generated extension-manifest inspection;
- dependency, duplicate-class, and `jdeps` inspection;
- a standalone extracted source build against the exact published stock host;
- Kafka lifecycle tests for generic host API changes; and
- paired CPU and allocation measurements when normal-path work changes.

Release archives contain exactly one lane-specific NAR. Compatibility metadata records the lane, exact NiFi version, parent NAR, bundle version, source revision, and build iteration.
