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

The source tree is standalone. It has its own parent POM, Maven Wrapper, tests, legal metadata, and host compatibility metadata. It does not depend on the standard JSON record-serialization implementation JAR or NAR. Its only direct NAR parent is `nifi-standard-shared-nar`.

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

## Build

From this directory, build one installation lane at a time:

```shell
./mvnw -B -ntp -Penhanced clean verify
./mvnw -B -ntp -Pstock -Dnifi.version=2.10.0 clean verify
```

The enhanced lane is the default for the NiFi reactor. `-Pall-lanes` is only a source verification profile. Its two outputs are mutually exclusive and must not be collected into one installation directory.

Build a portable release archive with one lane, its complete SBOM, compatibility manifest, legal files, and checksums:

```shell
./mvnw -B -ntp -Penhanced,release \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
./mvnw -B -ntp -Pstock,release -Dnifi.version=<exact-host-version> \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
```

Release outputs are written to `nifi-streaming-json-record-services-distribution/target/`:

- one lane-specific binary ZIP containing exactly one NAR, README, LICENSE, NOTICE, compatibility metadata, and CycloneDX JSON SBOM;
- one standalone source ZIP containing this Maven project and its wrapper;
- SHA-256 and SHA-512 files for both archives.

The release verifier rejects a binary ZIP containing multiple NARs or top-level JARs, an incomplete SBOM, or parent-NAR components in the SBOM.

Before publishing a build, assign a unique bundle version instead of replacing a binary with the same runtime identity:

```shell
./mvnw -B -ntp versions:set -DnewVersion=<unique-bundle-version> -DgenerateBackupPoms=false
./mvnw -B -ntp -Pstock -Dnifi.version=<exact-host-version> \
  -Dstreaming.json.source.revision=<source-revision> -Dbuild.iteration=<build-id> clean verify
```

Every NAR contains `META-INF/compatibility.properties` with the lane, bundle version, exact target NiFi version, parent NAR, source revision, and build iteration. The verify phase rejects incomplete packaged legal or compatibility metadata.

Normal NiFi `ProcessSession` streams and exact byte-array streams use the host's rewind contract for inferred schemas and do not create replay files. Recognition of `TaskTerminationInputStream` is intentionally limited to the exact framework class supplied by `ProcessSession`; custom code must not construct that framework wrapper around a finite-mark delegate. Other streams use bounded replay even when they report mark support, because ordinary buffered streams can invalidate a mark after their finite read limit. Replay stays in memory through 1 MiB, then uses a delete-on-close temporary file. `Maximum Schema Inference Replay Size` sets the per-reader bound and defaults to 1 GiB so deployments can set a lower concurrent temporary-storage budget. The Record Reader API defines its input-length argument as a hint, so the service does not reject a stream from that value alone. Larger generic streams must use a static or cached schema.

`Record Materialization Strategy` applies to inferred-schema reading. The default `Prefer Deferred` strategy avoids typed field conversion for eligible strict UTF-8, root-level inputs when no schema cache is configured and complete validated metadata is available. InputStream deferral is limited to 1,024 records, 16 MiB per record, and 64 MiB of record bytes; other encoding, parser, schema, cache, nested-field, and limit cases use eager or compatibility decoding. Ordinary InputStream raw-record capture is also limited to 16 MiB per record. Larger records continue through typed decoding without a reusable serialized form, so raw output is abandoned instead of retaining an unbounded byte copy. Deferred conversion failures can surface when a Record field is first accessed, so deferred readers advertise the retainable lifetime guarantee rather than claiming complete field conversion. Select `Eager` for inferred-schema processors that inspect or modify most records so conversion and its failures occur during sequential reading; this mode does not capture raw InputStream bytes. Both strategies expose the same Record values and writer compatibility.

Schema inference retains at most 10,000 distinct fields across the complete inferred schema by default, including nested records and array elements. `Maximum Schema Inference Fields` can raise or lower that bound. `Maximum JSON Nesting Depth` defaults to Jackson's standard depth of 1,000 and can be lowered for untrusted inputs. An explicit or cached schema remains the recommended path for exceptionally wide records.

## Install and rollback

1. Stop the target NiFi node.
2. Confirm the NAR lane and `target.nifi.version` in `META-INF/compatibility.properties` match the host.
3. Confirm no other NAR with `Nar-Id: nifi-streaming-json-record-services-nar` is installed.
4. Copy the single matching NAR to the NiFi `lib` directory.
5. Start NiFi and create the two streaming Controller Services explicitly. Existing `JsonTreeReader` and `JsonRecordSetWriter` services are unchanged.

For rollback, stop NiFi, remove the streaming NAR, restore the prior uniquely versioned streaming NAR if one was used, and restart. Flows must be switched back to an existing compatible reader/writer before permanently removing services they reference.

The stock lane works through the normal Record Reader/Writer contracts used by Kafka and other Record processors. The enhanced lane adds direct-byte Kafka dispatch when the host supports it; neither service depends on Kafka processor classes. Selecting the streaming reader through `ReaderLookup` remains functionally compatible but exposes only the standard `RecordReaderFactory` contract, so Kafka must reference `StreamingJsonRecordReader` directly to use the optional direct-byte path.
