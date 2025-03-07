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
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.nifi.RegistryClientsResult;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisabledOnOs(OS.WINDOWS)
public class TestRegistryClientResult {

    private ByteArrayOutputStream outputStream;
    private PrintStream printStream;

    @BeforeEach
    public void setup() {
        this.outputStream = new ByteArrayOutputStream();
        this.printStream = new PrintStream(outputStream, true);
    }

    @Test
    public void testWriteSimpleRegistryClientsResult() throws IOException {
        final FlowRegistryClientDTO r1 = new FlowRegistryClientDTO();
        r1.setName("Registry 1");
        r1.setType("Type 1");
        r1.setProperties(Map.of("url", "http://thisisalonglonglonglonglonglonglonglonglonguri.com:18080"));
        r1.setId(UUID.fromString("ea752054-22c6-4fc0-b851-967d9a3837cb").toString());

        final FlowRegistryClientDTO r2 = new FlowRegistryClientDTO();
        r2.setName("Registry 2 with a longer than usual name");
        r2.setType("Type 2");
        r2.setProperties(Map.of("url", "http://localhost:18080"));
        r2.setId(UUID.fromString("ddf5f289-7502-46df-9798-4b0457c1816b").toString());

        final FlowRegistryClientEntity clientEntity1 = new FlowRegistryClientEntity();
        clientEntity1.setId(r1.getId());
        clientEntity1.setComponent(r1);

        final FlowRegistryClientEntity clientEntity2 = new FlowRegistryClientEntity();
        clientEntity2.setId(r2.getId());
        clientEntity2.setComponent(r2);

        final Set<FlowRegistryClientEntity> clientEntities = new HashSet<>();
        clientEntities.add(clientEntity1);
        clientEntities.add(clientEntity2);

        final FlowRegistryClientsEntity flowRegistryClientsEntity = new FlowRegistryClientsEntity();
        flowRegistryClientsEntity.setRegistries(clientEntities);

        final RegistryClientsResult result = new RegistryClientsResult(ResultType.SIMPLE, flowRegistryClientsEntity);
        result.write(printStream);

        final String resultOut = outputStream.toString(StandardCharsets.UTF_8);

        final String expected = """

            #   Name                                   Type     Id                                     Properties                                                             \s
            -   ------------------------------------   ------   ------------------------------------   ---------------------------------------------------------------------  \s
            1   Registry 1                             Type 1   ea752054-22c6-4fc0-b851-967d9a3837cb   {url=http://thisisalonglonglonglonglonglonglonglonglonguri.com:18080}  \s
            2   Registry 2 with a longer than usu...   Type 2   ddf5f289-7502-46df-9798-4b0457c1816b   {url=http://localhost:18080}                                           \s

            """;

        assertEquals(expected, resultOut);
    }

}
