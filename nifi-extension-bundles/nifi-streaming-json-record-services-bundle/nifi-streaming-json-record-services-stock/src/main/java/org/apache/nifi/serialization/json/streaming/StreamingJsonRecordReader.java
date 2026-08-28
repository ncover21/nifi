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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"json", "streaming", "record", "reader", "parser"})
@CapabilityDescription("Reads one or more JSON objects or arrays as Records using tree-free token processing and the standard Record Reader InputStream contract. "
        + "Supports inferred, explicit, cached, and referenced schemas with correctness-preserving fallback for unsupported streaming cases.")
@SeeAlso(StreamingJsonRecordSetWriter.class)
public final class StreamingJsonRecordReader extends AbstractStreamingJsonRecordReaderService {
}
