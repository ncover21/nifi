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

# Streaming JSON Record Services

This independent extension bundle provides two opt-in NiFi Controller Services:

- `org.apache.nifi.serialization.json.streaming.StreamingJsonRecordReader`
- `org.apache.nifi.serialization.json.streaming.StreamingJsonRecordSetWriter`

The source tree is standalone. It has its own parent POM, Maven Wrapper, tests, legal metadata, and host compatibility metadata. The runtime core and NARs do not depend on the standard JSON record-serialization implementation JAR or NAR. Processor compatibility tests use the standard services as behavioral controls. The streaming NAR's only direct NAR parent is `nifi-standard-shared-nar`.

## Recommended configurations

For inferred-schema pass-through or grouping workloads, start with:

- Reader: `Schema Access Strategy = Infer Schema`, `Parsing Strategy = Standard`, and `Record Materialization Strategy = Prefer Deferred`.
- Writer: `Serialized JSON Input Handling = Enabled` and `Timestamp Representation = Automatic`.

This configuration prioritizes CPU efficiency and may preserve eligible input JSON exactly. Writer-side date/time formats and null suppression are not guaranteed for an eligible raw record. Set `Serialized JSON Input Handling = Disabled` when every record must be normalized according to writer properties.

For processors that inspect or modify most fields, such as `QueryRecord` and `UpdateRecord`, select `Record Materialization Strategy = Eager` when conventional read-time conversion and failure timing are preferred. Both materialization strategies produce normal NiFi Records and work with processors that accept `RecordReaderFactory` and `RecordSetWriterFactory` services.

Component behavior, examples, limits, and property interactions are documented in each Controller Service's Usage page. Maintainer-level execution, ownership, fallback, and classloader contracts are described in [docs/architecture.md](docs/architecture.md).

## Host lanes

| Lane | Host requirement | Reader entry point | Artifact |
| --- | --- | --- | --- |
| Stock | Exact unmodified supported NiFi release | Standard `RecordReaderFactory` InputStream API | `nifi-streaming-json-record-services-stock-nar/target/*.nar` |
| Enhanced | Exact NiFi build containing `ByteArrayRecordReaderFactory` and the record-lifetime API | Direct `byte[]` and standard InputStream APIs | `nifi-streaming-json-record-services-nar/target/*.nar` |

Both lanes contain the same core reader, writer, inference, capture, and serialization implementation. Only the thin reader adapter differs. They publish the same runtime NAR identity and must never be installed together.

Verified source targets:

| Lane | NiFi target | Status |
| --- | --- | --- |
| Stock | 2.10.0 | Standalone `clean verify` passed against published artifacts |
| Stock | 2.12.0-SNAPSHOT | Current reactor build passed |
| Enhanced | 2.12.0-SNAPSHOT with the generic byte/lifetime API changes | Current reactor build passed |

Compatibility is exact-version, not a general NiFi 2.x promise. Rebuild and test the bundle for every target release.

The stock lane is independently reproducible against published NiFi artifacts. The enhanced lane requires the exact matching host API artifacts to be available in Maven repositories or the local Maven repository; during host API development, build it within that NiFi reactor. A clean extracted source tree cannot build the enhanced lane from unpublished snapshot coordinates alone.

## Build

The build requires JDK 21. From this directory, use the included Maven Wrapper and build one installation lane at a time:

```shell
./mvnw -B -ntp -Penhanced -Dnifi.version=<matching-host-version> clean verify
./mvnw -B -ntp -Pstock -Dnifi.version=2.10.0 clean verify
```

The enhanced lane is the default for the NiFi reactor. `-Pall-lanes` is only a source verification profile. Its two outputs are mutually exclusive and must not be collected into one installation directory.

Run the representative Record-processor compatibility matrix for each lane:

```shell
./mvnw -B -ntp -Pprocessor-compatibility -Dnifi.version=2.10.0 clean verify
./mvnw -B -ntp -Pprocessor-compatibility-enhanced -Dnifi.version=<matching-host-version> clean verify
```

The matrix exercises pass-through, query, update, partition, merge, split, validation, and lookup behavior through the standard Record interfaces.

Build a portable release archive with one lane, its complete SBOM, compatibility manifest, legal files, and checksums:

```shell
./mvnw -B -ntp -Penhanced,release \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
./mvnw -B -ntp -Pstock,release -Dnifi.version=<exact-host-version> \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
```

Release outputs are written to `nifi-streaming-json-record-services-distribution/target/`:

- one lane-specific binary ZIP containing exactly one NAR, README, architecture documentation, LICENSE, NOTICE, compatibility metadata, and CycloneDX JSON SBOM;
- one standalone source ZIP containing this Maven project and its wrapper;
- SHA-256 and SHA-512 files for both archives.

The release verifier rejects a binary ZIP containing multiple NARs or top-level JARs, an incomplete SBOM, or parent-NAR components in the SBOM.

Before publishing a build, assign a unique bundle version instead of replacing a binary with the same runtime identity:

```shell
./mvnw -B -ntp versions:set -DnewVersion=<unique-bundle-version> -DgenerateBackupPoms=false
./mvnw -B -ntp -Pstock,release -Dnifi.version=<exact-host-version> \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
```

Every NAR contains `META-INF/compatibility.properties` with the lane, bundle version, exact target NiFi version, parent NAR, source revision, and build iteration. The verify phase rejects incomplete packaged legal or compatibility metadata.

Infer Schema scans the complete input before returning the first Record. Normal NiFi `ProcessSession` streams and enhanced direct-byte inputs can be rewound without a replay file. Other InputStreams are replayed with 1 MiB retained in memory before spilling to a delete-on-close file in the JVM temporary directory. `Maximum Schema Inference Replay Size` is a hard per-reader bound and defaults to 1 GiB; size temporary storage for the number of concurrently open readers or use an explicit or cached schema. Detailed thresholds and developer integration rules are documented in the Controller Service Usage and architecture pages.

## Install and rollback

1. Stop the target NiFi node.
2. Confirm the NAR lane and `target.nifi.version` in `META-INF/compatibility.properties` match the host.
3. Confirm no other NAR with `Nar-Id: nifi-streaming-json-record-services-nar` is installed.
4. Copy the single matching NAR to the NiFi `lib` directory.
5. Start NiFi and create the two streaming Controller Services explicitly. Existing `JsonTreeReader` and `JsonRecordSetWriter` services are unchanged.

For rollback, stop NiFi, remove the streaming NAR, restore the prior uniquely versioned streaming NAR if one was used, and restart. Flows must be switched back to an existing compatible reader/writer before permanently removing services they reference.

The stock lane works through the normal Record Reader/Writer contracts used by Kafka and other Record processors. The enhanced lane adds direct-byte Kafka dispatch when the host supports it; neither service depends on Kafka processor classes. Selecting the streaming reader through `ReaderLookup` remains functionally compatible but exposes only the standard `RecordReaderFactory` contract, so Kafka must reference `StreamingJsonRecordReader` directly to use the optional direct-byte path.
