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

The stock reader performs tree-free schema inference and record decoding through the standard `RecordReaderFactory` InputStream contract. The default Prefer Deferred materialization strategy preserves validated record bytes and avoids typed field conversion for eligible strict UTF-8, root-level inferred-schema inputs without a schema cache. InputStream deferral is limited to 1,024 records, 16 MiB per record, and 64 MiB of record bytes. Ordinary InputStream raw-record capture is also limited to 16 MiB per record; larger records continue through typed decoding without a reusable serialized form. Encoding, parser, schema, cache, nested-field, and limit cases that are not eligible use eager or compatibility decoding. Deferred conversion failures can surface when a Record field is first accessed, so deferred readers advertise a retainable lifetime rather than complete conversion. Select Eager when downstream processors inspect or modify most records and conventional read-time conversion is preferred; Eager inferred-schema InputStream reading does not capture raw record bytes.

Normal NiFi ProcessSession streams use the host rewind contract. Other inferred-schema InputStreams use bounded replay, with up to 1 MiB in memory before spilling to a delete-on-close temporary file. Maximum Schema Inference Replay Size controls the per-reader bound. Custom callers must not construct NiFi's TaskTerminationInputStream around a finite-mark delegate; that exact framework class is recognized as the standard ProcessSession stream.

Maximum Schema Inference Fields defaults to 10,000 distinct fields across the complete inferred schema, including nested records and array elements. Maximum JSON Nesting Depth defaults to 1,000. Lower either value for stricter untrusted-input bounds, or use an explicit schema for legitimately wider input.

Unsupported schemas use a correctness-preserving typed fallback. This mutually exclusive stock artifact does not implement the enhanced direct-byte reader capability.
