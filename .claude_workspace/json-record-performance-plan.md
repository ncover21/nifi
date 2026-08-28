# High-Performance JSON Record Path Plan

Status: completed optimization prototype and evidence base. The accepted next architecture is maintained in `streaming-json-controller-services-plan.md`, which extracts the optimized path into two opt-in Controller Services and restores legacy service selection.

## Objective

Build and validate a production-grade JSON Record Reader and Writer path for Apache NiFi that materially reduces CPU and allocation for one-JSON-object-per-message Kafka workloads while retaining compatibility with generic Record-aware processors and writers.

Baseline revision: `c4ca6c985b6a066edb1536a31b620ca02cea779b`

Runtime baseline:

- Java 21
- Maven 3.9.16
- NiFi 2.12.0-SNAPSHOT
- NIFI-15856 serialized JSON input handling present
- NIFI-16187 merged-schema Kafka grouping present

Customer workload:

- Primary topic: 162 partitions, about 2 kB uncompressed JSON objects, 182K average and 500K peak events per second.
- 350-700 MB/s uncompressed and 27 TB/day on the first topic; 40-60 TB/day is possible across planned topics.
- Production configuration target: `ConsumeKafka` with Infer Schema and Continue with Merged Schema.
- Optimize the one-object-per-Kafka-value path first. Treat root arrays and JSONL as compatibility workloads with separate memory reporting.

## Phase 1: Baseline and measurement harness

- Add a JMH module under `nifi-system-tests` so benchmark dependencies never enter production NARs.
- Benchmark the current tree inference, tree reader, generic JSON writer, serialized-form writer, and schema merging paths.
- Cover one-record Kafka-sized inputs and multi-record array/JSONL inputs.
- Capture throughput and `gc.alloc.rate.norm` using revision-and-iteration result names.
- Record JVM, operating system, corpus shape, fork count, warmup, and measurement settings with every result.
- Keep correctness tests separate from performance measurements.

Primary references:

- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/JsonRecordSource.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/JsonSchemaInference.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/JsonTreeRowRecordReader.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/WriteJsonResult.java`
- `nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/util/DataTypeUtils.java`

Verification:

- Build the benchmark jar.
- Run focused benchmark smoke tests.
- Run stable single-thread, multi-fork baseline measurements with GC profiling.

## Phase 2: Tree-free streaming schema inference

- Add a JSON token `RecordSource` that positions a Jackson parser at each logical record.
- Add a JSON-specific streaming inference engine that consumes tokens directly.
- Preserve current field order, numeric inference, temporal inference, arrays, nested records, choices, empty-array defaults, parsing strategies, constraints, and nested-field behavior.
- Preserve duplicate-key behavior or explicitly fall back when exact parity cannot be guaranteed.
- Keep `JsonRecordSource`, `JsonSchemaInference`, and generic `HierarchicalSchemaInference` intact for source compatibility and non-JSON callers.
- Wire the new engine into the appropriate JSON Controller Service without changing the `RecordReaderFactory` contract.

Primary references:

- `nifi-extension-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/main/java/org/apache/nifi/json/JsonTreeReader.java`
- `nifi-extension-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services-shared/src/main/java/org/apache/nifi/schema/inference/InferSchemaAccessStrategy.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-schema-inference-utils/src/main/java/org/apache/nifi/schema/inference/FieldTypeInference.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-schema-inference-utils/src/main/java/org/apache/nifi/schema/inference/HierarchicalSchemaInference.java`

Verification:

- Differential schema equality across every existing JSON inference fixture.
- Numeric-boundary, temporal, null, empty-array, nested-field, Unicode, malformed-input, duplicate-key, and constraint tests.
- Existing JSON reader and inference test suites.
- JMH comparison for single-message and batched inference, including allocation.

## Phase 3: Byte-oriented serialized JSON writer

- Extend JSON serialized-input handling to accept immutable UTF-8 byte representations without creating a whole-record UTF-16 `String`.
- Use Jackson's `SerializableString` raw-value path so the generator retains framing and output-context correctness.
- Preserve MIME, schema, pretty-print, timestamp representation, and scientific-notation eligibility checks.
- Add byte-oriented newline and scientific-notation checks without decoding.
- Retain the existing String path and generic field-by-field fallback.

Primary references:

- `nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/SerializedForm.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/WriteJsonResult.java`
- `nifi-extension-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-services/src/test/java/org/apache/nifi/json/TestWriteJsonResult.java`

Verification:

- Byte and String serialized forms produce identical JSON array and JSONL output.
- Pretty-print mismatch, scientific notation, schema mismatch, MIME mismatch, timestamp mode, disabled handling, compression, and mutation all use the correct path.
- Writer throughput and allocation comparison for String, byte, and generic forms.

## Phase 4: Streaming byte-backed reader

- Add an additive JSON reader implementation using Jackson tokens rather than `JsonNode` for Record construction.
- Retain immutable source bytes or slices when input ownership permits it.
- Produce records with byte-backed `SerializedForm` and a normal typed Record representation for generic consumers.
- Keep reader buffering bounded by the source value and use the existing tree reader as a fallback when optimized ownership or framing conditions are unavailable.
- Preserve root object, root array, JSONL, nested-field, type coercion, unknown-field, and malformed-record behavior.
- Preserve Kafka's existing per-message failure attribution and offset behavior. Fully validated readers may validate before returning records, while other readers continue to expose failures as records are consumed.
- For direct, uncached schema inference, retain validated record slices and defer the second value-building parse until a caller actually reads or mutates a field. Schema inspection and compatible serialized JSON writing must not materialize field values.
- Preserve eager materialization as the fallback for cached/external schemas, unsupported inferred schemas, transformed JSON, and any serialized-form eligibility ambiguity.

Primary references:

- `nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/RecordReader.java`
- `nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/MapRecord.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/AbstractJsonRowRecordReader.java`
- `nifi-extension-bundles/nifi-extension-utils/nifi-record-utils/nifi-json-record-utils/src/main/java/org/apache/nifi/json/JsonTreeRowRecordReader.java`

Verification:

- Differential record values and schemas across all existing reader fixtures.
- Exact malformed-input timing and original-byte preservation.
- Serialized-form invalidation after direct and nested mutations.
- Multi-record and reader-close lifetime tests.
- Reader and end-to-end reader/writer JMH measurements with allocation profiling.

## Phase 5: Schema canonicalization and Kafka grouping

- Measure structural shape cardinality before selecting cache policy.
- Canonicalize repeated inferred schemas using a collision-checked, bounded cache.
- Avoid cumulative immutable schema merges for repeated shapes in a poll group.
- Resolve writer schemas once only where the strategy and attributes make the result stable.
- Keep group ordering, topic/partition/header keys, offsets, timestamps, counts, provenance, and failure relationships unchanged.
- Keep all caches bounded by entry count and estimated weight; expose hit, miss, eviction, and fallback behavior.

Primary references:

- `nifi-extension-bundles/nifi-kafka-bundle/nifi-kafka-processors/src/main/java/org/apache/nifi/kafka/processors/consumer/convert/MergeSchemaGrouping.java`
- `nifi-extension-bundles/nifi-kafka-bundle/nifi-kafka-processors/src/main/java/org/apache/nifi/kafka/processors/consumer/convert/CreateNewFlowFileGrouping.java`
- `nifi-extension-bundles/nifi-kafka-bundle/nifi-kafka-processors/src/main/java/org/apache/nifi/kafka/processors/consumer/convert/AbstractRecordStreamKafkaMessageConverter.java`
- `nifi-commons/nifi-record/src/main/java/org/apache/nifi/serialization/record/util/DataTypeUtils.java`

Verification:

- Existing grouping unit and Kafka integration tests.
- Stable, mixed, and adversarial high-cardinality schemas.
- Peak live heap per poll group and old-generation plateau.
- Identical grouping boundaries, schemas, FlowFile attributes, and output values.

## Phase 6: Optional direct-byte and batch capability

- Add a generic optional byte-array reader-factory capability only if input-copy cost remains material after the earlier phases.
- Let Kafka pass its existing value array directly while all other factories retain the current InputStream fallback.
- Add a batch accumulator only if per-message schema construction and merge remain dominant.
- Keep JSON-specific inference and writing out of `ConsumeKafka`; the processor only orchestrates optional generic capabilities.
- Preserve exact per-message failure attribution and offset advancement points.
- Preserve existing transactional session and Kafka commit behavior.

Primary references:

- `nifi-extension-bundles/nifi-standard-services/nifi-record-serialization-services-bundle/nifi-record-serialization-service-api/src/main/java/org/apache/nifi/serialization/RecordReaderFactory.java`
- `nifi-extension-bundles/nifi-kafka-bundle/nifi-kafka-processors/src/main/java/org/apache/nifi/kafka/processors/ConsumeKafka.java`
- `nifi-extension-bundles/nifi-kafka-bundle/nifi-kafka-processors/src/main/java/org/apache/nifi/kafka/processors/consumer/convert/AbstractRecordStreamKafkaMessageConverter.java`

Verification:

- Full Kafka integration matrix for USE_VALUE, INJECT_OFFSET, USE_WRAPPER, and INJECT_METADATA.
- Merged and create-new FlowFile strategies.
- Parse failures at start, middle, and end of values and poll batches.
- Offset commit, rollback, rebalance, replay, partition ordering, and concurrent-task tests.
- End-to-end Kafka throughput, CPU/core, allocation, GC, live heap, and RSS measurements.

## Acceptance gates

- No correctness differences in supported modes.
- No increase in allocated bytes per record, peak live heap, total RSS, GC CPU, or tail pauses at equal workload and concurrency.
- CPU/core is the primary customer gate because production nodes pin near 95% CPU and node count is driven by compute. Controller-Service path target: at least 2x records per core or at most 50% baseline CPU per record for representative Infer Schema plus merged-schema JSON output.
- Batch-aware stretch target: approximately 3x records per core or removal of most of the gap to DEMARCATOR mode.
- Every optimized path has an explicit, counted generic fallback.
- Parser replacement is considered only after profiles show tokenization remains a dominant cost.

## Execution results

The iteration history below is retained for traceability. The definitive final-artifact results and verification are in **Final verification snapshot** at the end of this file; earlier benchmark values are not acceptance results.

Implementation:

- Added streaming token-based JSON schema inference and wired it into the stock JSON Tree reader while retaining the existing JSON Path and subclass inference paths.
- Added a strict byte-backed streaming row reader with transparent tree fallback for unsupported schemas, lenient parsing, conversion failures, duplicate keys, and alias conflicts. Unknown fields are skipped in one pass and disable raw serialized-form reuse so dropped fields cannot leak into output.
- Added a validated deferred Record path for direct uncached inference. Inference retains record byte slices and formatting/null metadata; schema inspection and compatible JSON writing do not build field values, while any field access or mutation transparently materializes the normal typed streaming Record.
- Added UTF-8 serialized JSON values and direct raw-value writer output without whole-record String decoding.
- Added an optional `ByteArrayRecordReaderFactory` capability plus default schema-access, record-source, and token-parser byte methods; non-capable and non-JSON implementations retain InputStream behavior.
- Added explicit reader lifetime modes so Kafka can retain reusable streaming records safely without staging every logical record in a message.
- Added bounded per-group distinct-schema tracking so repeated non-recursive Kafka schemas merge once while adversarial cardinality falls back after 64 shapes. Recursive schemas use identity-only matching before merge.
- Added schema-compatible serialized writing for validated Records under merged top-level schemas. Primitive widening stays raw; nullable missing fields are either suppressed according to configuration or appended as `null` without materializing the Record. Nested structural changes remain on the typed fallback.
- Kafka adds records to groups as they are read. `STREAMING` records are copied only when the selected grouping strategy retains them; `RETAINABLE` and `FULLY_VALIDATED` records are used directly. Homogeneous multi-record values still resolve the writer schema once.
- Preserved baseline failure routing: checked reader, schema, conversion, and grouping/writer failures route the original value to `parse.failure` and advance its offset. A valid prefix from a malformed multi-record value can therefore remain grouped. Runtime and finalization failures abort open groups and roll back.
- Removed iterator allocation from composite temporal matchers used by schema inference.
- Preserved the existing JsonTreeReader subclass inference hook and placed the JMH module behind the `benchmarks` Maven profile.

JMH environment:

- Revision: `c4ca6c985b6a066edb1536a31b620ca02cea779b`
- JVM: Amazon Corretto 21.0.6
- JMH: 1.37, 1 thread, 2 forks, 3 x 1-second warmup, 5 x 1-second measurement, GC profiler
- Corpus: representative 541-byte Kafka JSON object with nested records, maps, arrays, numbers, temporal text, and strings

Results:

| Path | Time | Allocation |
| --- | ---: | ---: |
| Untouched tree infer/read/write baseline (`jmh-c4ca6c985b6a-01.json`) | 6.683 us/op | 31,224 B/op |
| Streaming infer/read/byte write before single-record inference fast path (`jmh-c4ca6c985b6a-14.json`) | 3.341 us/op | 12,352 B/op |
| Streaming infer/read/byte write (`jmh-c4ca6c985b6a-20.json`) | 3.159 us/op | 10,064 B/op |
| Current tree infer/read/write, 2 kB (`jmh-c4ca6c985b6a-22.json`) | 9.617 us/op | 31,824 B/op |
| Current streaming infer/read/byte write, 2 kB (`jmh-c4ca6c985b6a-22.json`) | 5.024 us/op | 15,208 B/op |
| Streaming infer/read/byte write after line-state propagation, 2 kB (`jmh-c4ca6c985b6a-24.json`) | 4.204 us/op | 15,216 B/op |
| Validated serialized-record upper-bound control, 2 kB (`jmh-c4ca6c985b6a-27-control.json`) | 2.221 us/op | 9,076 B/op |
| Validated deferred Record implementation, 2 kB (`jmh-c4ca6c985b6a-32-validated.json`) | 2.350 us/op | 9,312 B/op |
| Byte-backed tree read, unknown first field (`jmh-c4ca6c985b6a-17-first.json`) | 1.773 us/op | 6,680 B/op |
| Streaming read, unknown first field (`jmh-c4ca6c985b6a-17-first.json`) | 1.463 us/op | 4,268 B/op |
| Byte-backed tree read, unknown last field (`jmh-c4ca6c985b6a-17-last.json`) | 1.666 us/op | 5,848 B/op |
| Streaming read, unknown last field (`jmh-c4ca6c985b6a-17-last.json`) | 1.267 us/op | 3,696 B/op |

- Composed infer/read/write microbenchmark: 2.12x throughput, 52.7% lower latency, 67.8% lower allocation. This does not include Controller Service lifecycle, Kafka polling, session, provenance, or broker overhead.
- On the customer-representative 2 kB parameter, the current streaming path is 1.91x faster and allocates 52.2% less than the current tree path. This is the same composed microbenchmark scope.
- Propagating Jackson's record line state removed a second full-payload newline scan and reduced the 2 kB streaming path by 16.3%, from 5.024 to 4.204 us/op, with allocation effectively unchanged.
- A control that writes the already validated serialized record without constructing field values measured 2.221 us/op and 9,076 B/op. This established the design upper bound before production implementation.
- The implemented validated deferred Record path measured 2.350 us/op and 9,312 B/op: 44.1% less CPU time and 38.8% less allocation than the eager streaming path, and 4.09x the current tree-path throughput at 2 kB. It remains within 5.8% of the upper-bound control.
- The Kafka-shaped stable-schema harness measured 20.851 us and 32,226 B per record for the InputStream/tree fallback, 7.479 us and 25,602 B for eager streaming, and 5.652 us and 20,796 B for the validated path. This includes the real Kafka converter, reader/writer services, offsets, attributes, ten partition groups, and writer finalization, but excludes ProcessSession, provenance, broker, and network I/O.
- With schema drift inside partition groups, the initial validated path measured 9.426 us and 29,867 B per record because the merged writer schema triggered materialization. Schema-compatible raw output with top-level null injection reduced the three-fork result to 6.578 us and 21,738 B: 30.2% less CPU and 27.2% less allocation, while preserving default missing-field output. Nested structural evolution retains the typed fallback.
- A temporal-length precheck experiment reduced composed validated allocation to 7,440 B but did not improve CPU and slowed the eager path by 4.9%. It was reverted because CPU/core is the primary gate.
- The corrected JFR profile shows Jackson string decoding during schema inference as the largest remaining CPU sample at 31.1%; the reader parse and serialized-form newline scan are no longer primary hot spots.
- The single-record inference fast path reduced the prior final path by another 8.5% latency and 18.8% allocation.
- Unknown-field streaming is 17.5-23.9% faster than the byte-backed tree reader and allocates 36.1-36.8% less, without a second parse.
- With independently inferred structural instances, safe distinct-shape grouping reduced alternating two-schema work from 0.496 us/op and 1,481 B/op to 0.183 us/op and 15 B/op per record (`jmh-c4ca6c985b6a-22.json`).
- Byte serialized writer: 0.354 us/op and 984 B/op in the focused run.
- Batch records retain one shared source payload rather than copying it per record; a regression test verifies source-array identity for every record slice.

Retained-memory probe:

- JOL reachable Record graph at 10,000 independent messages: 57,725,864 bytes tree versus 44,685,648 bytes streaming before write, 22.6% lower.
- JOL reachable Record graph at 1,000 records in one array payload: 3,835,128 bytes tree versus 2,510,024 bytes streaming before write, 34.6% lower.
- Preliminary forced-GC observations for the isolated 10,000-message synthetic workload showed 27.5% less live heap, a 16.2% lower RSS plateau, and 18.4% lower NMT committed memory. The command output was not preserved, so these are not acceptance evidence.
- The same synthetic JFR workload recorded 390 versus 242 GC pauses and 7.53 versus 4.43 seconds of GC user CPU. Tail-pause percentiles remain inconclusive without repeated process forks.
- The probe and exact reproduction commands are documented in the benchmark module README. JOL values are reachable closed-Record graph sizes, not peak or dominator retained sizes; RSS and JFR include probe overhead.
- The converter does not stage the complete logical-record list. Retaining group strategies can still accumulate records by design; only reusable `STREAMING` records are copied before reader advancement. Large-array end-to-end peak memory remains an open production gate.

Verification:

- Record serialization reactor: 603 tests passed, 0 failures, 0 errors, 5 skipped after the validated deferred Record changes.
- Kafka processors reactor: 47 tests passed, 0 failures, 0 errors after the latest review fixes.
- Focused final reader/subclass compatibility suite: 19 tests passed.
- Kafka batching stress coverage processes 10,000 logical records without whole-message staging and verifies one writer-schema resolution for a homogeneous message.
- High-cardinality grouping coverage verifies exact sequential-merge parity beyond the 64-schema tracking bound.
- An earlier 43-module affected reactor verification build passed with the opt-in benchmark profile; the final expanded reactor result is recorded below.
- Review-driven regressions now cover same-name evolved recursive schemas, repeated shapes beyond the bounded tracker, exact parse-failure payload and offset handling, baseline checked grouping-failure routing, and runtime/finalization abort behavior.
- Deferred-path regressions cover no-materialization raw writes, field materialization, mutation invalidation, multi-record byte slices, null fallback, and schema-cache eager fallback.
- `git diff --check` passed.

Integration correction:

- The apparent Kafka integration API incompatibilities were stale and orphaned bytecode in local `target` output, not source incompatibilities. A clean current reactor build removed the obsolete classes.
- The stock `ConsumeKafkaMergeSchemaIT` Testcontainers suite then passed all seven broker-backed cases without source accommodations. No unrelated Kafka integration files are modified.

## Final verification snapshot

Final implementation details added during review:

- Added the source-compatible `RecordReader.RecordHandlingMode` contract: `STREAMING`, `RETAINABLE`, and `FULLY_VALIDATED`.
- Preserved reusable-reader behavior for the default streaming mode by copying records only when a grouping strategy retains them.
- Preserved baseline checked-failure routing to `parse.failure` with offset advancement. Runtime and group-finalization failures abort all open groups and roll back.
- Removed per-message converter state, per-group `AtomicInteger`, capturing grouping lambdas, and per-record deferred materializer lambdas.
- Restricted merged raw reuse to exact data types. Numeric widening uses typed output, preserving lexical and conversion semantics including negative zero and precision boundaries.
- Added explicit UTF-8 eligibility checks. UTF-8 BOM and UTF-16/UTF-32 JSON bytes, with and without BOM and in both byte orders, take the normalized typed fallback.
- Direct inferred arrays above 1,024 records use fully validated eager streaming. Smaller direct inferred inputs use deferred validation. Static/external-schema arrays are processed as read without whole-message staging.

Artifact identity:

- Baseline revision: `c4ca6c985b6a066edb1536a31b620ca02cea779b`
- Final benchmark JAR SHA-256 for the original final iteration: `e3987c45f7a4773d3f54c644538cffffdbe9959b8c99da99daf28848678516ba`
- Java 21.0.6, JMH 1.37, one thread, two forks, 3 x 1-second warmup, 5 x 1-second measurement, GC profiler

Final 2 kB composed infer/read/write result (`jmh-c4ca6c985b6a-e3987c45f7a4-composed-01.json`):

| Path | Time/record | Allocation/record |
| --- | ---: | ---: |
| Tree | 9.527 us | 31,784 B |
| Eager streaming | 4.310 us | 15,256 B |
| Validated deferred | 2.186 us | 9,272 B |

- Validated versus tree: 4.36x throughput, 77.1% less time/record, 70.8% less allocation.
- Validated versus eager: 1.97x throughput, 49.3% less time/record, 39.2% less allocation.

Final Kafka converter matrix (`jmh-c4ca6c985b6a-6a142da0b049-kafka-matrix-01.json`):

| Access | Shape | Tree | Eager | Validated |
| --- | --- | ---: | ---: | ---: |
| Infer | Stable time / allocation | 22.375 us / 31,191 B | 7.442 us / 21,618 B | 6.261 us / 16,732 B |
| Infer | Drifting time / allocation | 13.281 us / 34,924 B | 11.740 us / 32,783 B | 5.597 us / 21,570 B |
| Static | Stable time / allocation | 36.934 us / 62,080 B | 21.590 us / 55,242 B | 21.681 us / 55,502 B |
| Static | Drifting-fixture time / allocation | 35.418 us / 61,641 B | 21.430 us / 55,068 B | 21.209 us / 55,372 B |

- Intended infer/stable path: validated is 3.57x tree throughput and allocates 46.4% less.
- Intended infer/drifting path: validated is 2.37x tree throughput and allocates 38.2% less.
- Static-schema validated/eager results are effectively identical, as the deferred optimization is intentionally restricted to direct uncached inference.
- These values are converter time/record, not measured process CPU. Cross-fixture stable/drifting comparisons are not meaningful; compare reader modes within a fixture.

Null-heavy infer/stable edge case (`jmh-c4ca6c985b6a-6a142da0b049-kafka-null50-01.json`):

- 50% null records with scientific-notation scanning enabled.
- Tree: 16.883 us and 30,735 B per record.
- Eager: 9.671 us and 25,454 B per record.
- Validated: 5.341 us and 16,689 B per record.
- Validated versus tree: 3.16x throughput and 45.7% less allocation.
- Validated versus eager: 1.81x throughput and 34.4% less allocation.

Broker-backed 162-partition processor CPU probe (`cpu-c4ca6c985b6a-e3987c45f7a4-*-01.json`):

- Real Testcontainers Kafka 4.3.1 broker, `ConsumeKafka`, Kafka 3 Controller Service, JSON reader/writer services, Infer Schema, Continue with Merged Schema, 2 kB values, 50,000 records per iteration, and five iterations per reader mode.
- A 50,000-record untimed pass validates every complete output value, unique ID, deterministic field, null, padding, and group-local merged firmware value/null/missing behavior before measurement. Producers are also outside timed intervals.
- Processor-thread CPU and allocation are measured inside `ConsumeKafka.onTrigger()`. Total JVM CPU and wall drain time are retained as noisier end-to-end secondary signals. `TestRunner` still uses mock session, repository, and provenance implementations.

| Shape | Reader | Processor CPU/record | Processor allocation/record | Wall/record |
| --- | --- | ---: | ---: | ---: |
| Stable | Tree | 26.755 us | 37,711 B | 36.338 us |
| Stable | Eager | 10.135 us | 27,605 B | 18.700 us |
| Stable | Validated | 6.797 us | 22,704 B | 15.560 us |
| Drifting | Tree | 15.609 us | 36,850 B | 24.429 us |
| Drifting | Eager | 14.160 us | 35,002 B | 23.212 us |
| Drifting | Validated | 7.201 us | 23,320 B | 16.598 us |

- Stable validated versus tree: 3.94x records per processor CPU-second, 74.6% lower processor CPU/record, 39.8% lower processor allocation, and 2.34x wall throughput.
- Stable validated versus eager: 1.49x records per processor CPU-second, 32.9% lower processor CPU/record, and 17.8% lower processor allocation.
- Drifting validated versus tree: 2.17x records per processor CPU-second, 53.9% lower processor CPU/record, 36.7% lower processor allocation, and 1.47x wall throughput.
- Drifting validated versus eager: 1.97x records per processor CPU-second, 49.1% lower processor CPU/record, and 33.4% lower processor allocation.
- Against the pre-review runs, final processor CPU medians changed by at most 4.0% and allocation by at most 0.4%. Compatibility and performance did not regress after review findings.

Final tests and build:

- Record serialization services: 626 tests passed, 5 skipped.
- Kafka processors: 59 tests passed, 0 skipped.
- Kafka merged-schema integration: 7 broker-backed tests passed, 0 skipped.
- Benchmark mode and schema-merge parity: 3 tests passed.
- Focused final JSON tests: 90 passed.
- Focused final converter/grouping tests: 19 passed.
- Affected 47-module `-Pbenchmarks` reactor build: passed.
- `git diff --check`: passed.
- Initial compatibility review: no findings; the later targeted subclass/API audit and its fixes are recorded below.
- Final performance/reuse/simplicity review: no actionable high-priority findings.

Post-review compatibility audit:

- Found that inherited `JsonTreeReader` subclasses could enter JSON streaming inference and restored their legacy source/tree hooks. `YamlTreeReader` now has explicit byte-reader regression coverage.
- Reproduced a source ambiguity caused by a byte-array `RecordReaderFactory` overload in the existing `TestPutTCP` source. Removed the overload and introduced the distinctly named optional `ByteArrayRecordReaderFactory.createRecordReaderFromBytes` capability instead.
- `RecordReaderFactory` remains unchanged for existing callers and implementations. Kafka detects the optional capability and otherwise invokes the existing four-argument InputStream method.
- A seven-JAR `japicmp` 0.23.1 comparison against the exact baseline passed with binary- and source-incompatibility failures enabled.
- A 77-module standard-processors reactor compiled all 150 test sources and passed 69 selected record-processor tests. The final full record-serialization and Kafka processor reactors passed 626 and 59 tests, respectively.
- Compatibility artifact `f886dac2998d326af1e8d44982937726f7890d1cb2de74055a95cc023e04fee9` retained the composed allocation results within 0.1% and time within 2.7%. The Kafka infer/stable comparison measured 22.196 us and 31,208 B per tree record versus 5.416 us and 16,700 B per validated record, with no measured regression.

Remaining production acceptance gate:

- Run a deployed NiFi node with durable FlowFile/content repositories, provenance, concurrent tasks, and customer-representative 182K average and 500K peak arrival rates while measuring CPU/core, records/core, JFR, GC CPU, live heap, RSS, and tail pauses.
- The broker-backed `TestRunner` probe validates the processor/controller-service path and 162-partition behavior, but it is not a substitute for that deployment test.

## Deep multi-angle audit snapshot

Three independent final reviews covered public/source compatibility and Controller Service subclass hooks; JSON correctness and differential edge cases; Kafka failure, lifecycle, and record-retention behavior; benchmark fidelity and hot-path allocation; and reuse/simplification. Every concrete finding was addressed and re-reviewed. No actionable code finding remains.

Audit changes include recursive last-key-wins duplicate handling, exact recursion-depth accounting through arrays and maps, preservation of legacy nested XML record metadata, case-insensitive content-schema references, exact-class optimization gates for existing JSON/YAML/JsonPath extension hooks, complete grouping cleanup on checked and runtime begin failures, retention-safe copying for collections and every Java array category, and schema-merge reuse that preserves nested metadata. Hot paths now avoid duplicate map probes, primitive-array boxing, and exception-driven reference-array copying. The benchmark modes now exercise the production reader and inference paths directly.

Final evidence:

- Final audit artifact SHA-256: `85145acf1e509e2adf4d4c2b3ec6f92b10190f2c713ef3bb9146ee60444494e5`.
- Full 33-module serialization reactor: 626 passed, 5 skipped.
- Full 41-module Kafka reactor: 59 passed.
- Affected 47-module `-Pbenchmarks` package reactor: passed.
- Seven-artifact `japicmp` binary/source gate against baseline `c4ca6c985b6a066edb1536a31b620ca02cea779b`: passed.
- Final focused reader audit: 62 passed; final converter/grouping audit: 29 passed; benchmark mode/parity suite: 3 passed.
- `git diff --check`: passed.

Final audit composed result (`jmh-c4ca6c985b6a-85145acf1e50-audit2-composed-01.json`): tree 9.736 us and 33,960 B per record; eager 4.563 us and 15,288 B; validated 2.253 us and 9,368 B. Validated is 4.32 times tree throughput with 72.4% less allocation, and 2.03 times eager throughput with 38.7% less allocation.

Final Kafka infer result (`jmh-c4ca6c985b6a-85145acf1e50-audit2-kafka-infer-02.json`, with validated/stable conservatively replaced by the complete rerun `jmh-c4ca6c985b6a-85145acf1e50-audit2-kafka-validated-stable-03.json`):

| Shape | Tree | Eager | Validated |
| --- | ---: | ---: | ---: |
| Stable | 22.896 us / 33,269 B | 7.794 us / 21,692 B | 5.882 us / 16,860 B |
| Drifting | 13.513 us / 36,917 B | 11.600 us / 32,864 B | 5.345 us / 21,599 B |

The validated/stable confidence interval is wide because one measurement included a 2.579-second host GC pause; the aggregate above deliberately retains that outlier. Even conservatively, validated is 3.89 times tree throughput and allocates 49.3% less in the stable fixture. In the clean drifting fixture it is 2.53 times tree throughput and allocates 41.5% less.

Compared with the pre-audit compatibility artifact, optimized composed CPU changed by +3.1% for eager and +0.8% for validated; allocation changed by +0.3% and +1.0%. The legacy tree allocation increase is isolated to reconstruction needed to restore exact nested-schema metadata. The CPU-primary optimized path has no material regression and remains substantially ahead of both compatibility paths.
