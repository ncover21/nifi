<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Record Serialization Benchmarks

This opt-in module measures JSON schema inference, Record reading, Record writing, and schema merging without adding benchmark dependencies to production NARs.

## Build

Run from the repository root:

```shell
./mvnw -Pbenchmarks -pl nifi-system-tests/nifi-record-serialization-benchmarks -am \
    -DskipTests -Dskip.nar package
```

The shaded benchmark jar is written to `nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar`.

## JMH

Use the source revision, shaded benchmark artifact hash, and an iteration in every result name so dirty-tree builds remain separate and reproducible:

```shell
REVISION=$(git rev-parse --short=12 HEAD)
ARTIFACT=$(shasum -a 256 nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar | cut -c1-12)
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'StreamingJsonServiceInputStreamBenchmark.convertSingleRecord|StreamingJsonServiceInputStreamBenchmark.convertRecordBatch' \
    -prof gc -rf json \
    -rff "nifi-system-tests/nifi-record-serialization-benchmarks/target/jmh-${REVISION}-${ARTIFACT}-01.json"
```

The benchmark annotations define two forks, three one-second warmups, and five one-second measurements. The `targetRecordBytes` parameter covers the historical roughly 512-byte corpus and the customer-representative 2 kB corpus. Override those settings on the command line only for smoke testing, and label the result accordingly.

`StreamingJsonServiceInputStreamBenchmark` measures the production streaming Controller Services with direct bytes, exact byte-array InputStreams, finite-mark InputStreams that must use bounded replay, and the legacy pair. `recordOperation` separates `PASS_THROUGH`, `READ_ONE_FIELD`, and `MUTATE_ONE_FIELD`. `materializationStrategy` selects `DEFERRED` or `EAGER`. Its timed output only counts serialized bytes, so it isolates reader/writer CPU and allocation; it does not model Content Repository I/O or output payload copying.

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'StreamingJsonServiceInputStreamBenchmark.convertRecordBatch' \
    -p entryPoint=FINITE_MARK_STREAM -p schemaAccess=INFER \
    -p framing=ARRAY -p targetRecordBytes=2048 \
    -p recordOperation=PASS_THROUGH -p materializationStrategy=DEFERRED -prof gc
```

`StreamingJsonContentClaimBenchmark` measures the real NiFi `TaskTerminationInputStream -> ContentClaimInputStream -> ContentRepository` read chain. It uses 2 KiB records and FlowFiles containing 1, 100, or 600 records, which are approximately 2 KiB, 200 KiB, and 1.2 MiB. `PRODUCTION` preserves the streaming reader's 1,000,000-byte mark limit: the two smaller inputs reset from `BufferedInputStream`, while the 1.2 MiB input reopens the repository. `UNTRUSTED_SUBCLASS_REPLAY` wraps the same repository stream in an unrecognized subclass and verifies the bounded replay fallback instead of trusting custom mark/reset behavior. Fixture validation asserts the exact repository read count before measurement. The repository stores bytes in memory, so the benchmark measures NiFi stream, parser, reader, writer, and replay CPU rather than disk latency.

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'StreamingJsonContentClaimBenchmark.convertFlowFile' \
    -p flowFileRecords=1,100,600 -p rewindMode=PRODUCTION,UNTRUSTED_SUBCLASS_REPLAY -prof gc
```

`KafkaJsonRecordBenchmark` adds the real Kafka Record converter, configured JSON reader and writer Controller Services, message attributes, offset tracking, ten partition groups, production-equivalent schema merging, writer creation, and output finalization. `STABLE` uses one schema; `DRIFTING` distributes optional-field records across partition groups so merged writers see both shapes. `INFER` with `DRIFTING` is the customer-shaped inferred/merged case; `STATIC` is a typed static-schema control. `nullPercentage` accepts record percentages such as 0, 1, 10, and 50, and `allowScientificNotation` isolates the raw-value notation scan. `serializedJsonInputHandling=ENABLED` permits eligible raw JSON reuse and is the acceptance default; `DISABLED` forces typed materialization and serialization as a control. Always pin and report this parameter because it materially changes the measured path.

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'KafkaJsonRecordBenchmark.convertBatch' -prof gc -rf json \
    -p serializedJsonInputHandling=ENABLED \
    -rff "nifi-system-tests/nifi-record-serialization-benchmarks/target/jmh-${REVISION}-${ARTIFACT}-kafka-01.json"
```

Run focused configurations with command-line parameters instead of expanding the default cross-product:

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'KafkaJsonRecordBenchmark.convertBatch' -p schemaAccessMode=STATIC -p schemaMode=STABLE \
    -p nullPercentage=0 -p allowScientificNotation=false \
    -p serializedJsonInputHandling=ENABLED -prof gc
```

Run the same fixture with forced typed materialization as an explicit control:

```shell
java -jar nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    'KafkaJsonRecordBenchmark.convertBatch' -p schemaAccessMode=INFER -p schemaMode=DRIFTING \
    -p nullPercentage=0 -p allowScientificNotation=false \
    -p serializedJsonInputHandling=DISABLED -prof gc
```

This is a converter benchmark, not a broker or full NiFi benchmark. It excludes Kafka polling, `ProcessSession`, provenance, FlowFile/content repositories, network I/O, and concurrent tasks. Preserve those boundaries when reporting its results.

## Broker-backed processor CPU probe

`KafkaJsonProcessorCpuProbe` runs the real `ConsumeKafka` processor, Kafka 3 Controller Service, JSON reader and writer Controller Services, and a Testcontainers Kafka broker. It configures Infer Schema with Continue with Merged Schema and validates the exact consumed record count, absence of parse failures, and representative output fields. Producers finish before each timed interval, and a 50,000-record untimed warmup precedes measurement.

Run the legacy and streaming service pairs in separate JVMs and retain the source revision and benchmark JAR hash in each result name:

```shell
java -cp nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    -Dprobe.revision="$(git rev-parse HEAD)" -Dprobe.runLabel=stable-01 \
    org.apache.nifi.json.benchmark.KafkaJsonProcessorCpuProbe \
    LEGACY 50000 162 5 \
    "nifi-system-tests/nifi-record-serialization-benchmarks/target/cpu-${REVISION}-${ARTIFACT}-legacy-01.json"
```

The arguments are `MODE RECORDS PARTITIONS ITERATIONS OUTPUT_FILE [NULL_PERCENTAGE] [DRIFTING]`. Modes are `LEGACY` and `STREAMING`; both instantiate the actual Controller Service pair. Pass `0 true` for the optional arguments to exercise schema drift. `probe.revision` is required; `probe.runLabel` defaults to the original output filename. The result embeds both values, the benchmark JAR SHA-256, JVM arguments, available processors, Kafka image, and pinned polling and grouping controls.

Use the median of iterations two through five as the steady-state comparison. Processor-thread CPU and allocation are measured inside `ConsumeKafka.onTrigger()` and provide the cleanest reader-mode signal. Total process CPU includes broker-client threads, JIT compilation, GC, and the test harness; wall time captures the complete drain interval. The full 50,000-record warmup also runs correctness validation outside the timed intervals, verifying unique IDs and complete deterministic record values. Preserve all raw iterations and report both measurement scopes and the validation summary.

This is a real broker and processor integration probe, but `TestRunner` supplies mock session, repository, and provenance implementations. It does not model a deployed NiFi node, concurrent tasks, durable FlowFile/content repositories, multi-node coordination, broker network distance, or customer arrival rates. Those remain production acceptance tests.

## Retained heap and process memory

`JsonRecordRetainedHeapProbe` compares the actual legacy and streaming Controller Service pairs for independent Kafka-style messages and one array-backed message. JOL reports the reachable closed-Record graph before and after writing, plus a synthetic graph that also includes source payloads. The probe asserts that every streaming serialized form shares its original source array and remains deferred after raw writing.

Run all default sizes:

```shell
java -Djdk.attach.allowAttachSelf=true -Djol.magicFieldOffset=true \
    -cp nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    org.apache.nifi.json.JsonRecordRetainedHeapProbe
```

Run one isolated 10,000-message mode and retain the graph long enough to inspect RSS, native memory, or a JFR recording:

```shell
java -Xms64m -Xmx512m -XX:NativeMemoryTracking=summary \
    -XX:StartFlightRecording=filename=target/streaming-many-10000.jfr,settings=profile \
    -Djdk.attach.allowAttachSelf=true -Djol.magicFieldOffset=true \
    -Dprobe.mode=STREAMING -Dprobe.scenario=MANY_MESSAGES -Dprobe.holdMillis=30000 \
    -cp nifi-system-tests/nifi-record-serialization-benchmarks/target/benchmarks.jar \
    org.apache.nifi.json.JsonRecordRetainedHeapProbe 10000
```

Use separate JVMs for legacy and streaming measurements. Record the Java version, operating system, heap settings, payload count, scenario, and probe mode with the results. JOL reports a reachable graph, not a dominator-tree retained size. Process RSS and sampled JFR allocation results include the probe and JOL overhead, so compare identical isolated workloads.

While the process is holding the graph, capture the process and JVM views using the PID printed by the probe:

```shell
ps -o pid,rss,vsz,command -p PID
jcmd PID GC.heap_info
jcmd PID VM.native_memory summary
```

The synthetic graph measurement is not a process peak: the reader and parser have already closed. It is an upper-bound experiment that keeps both Records and every source payload reachable. The current Kafka converter processes retainable records as they are read and does not stage a whole message; retaining grouping strategies copy only records from readers that declare streaming lifetimes. The probe is not an end-to-end Kafka, ProcessSession, provenance, or concurrent-task benchmark.
