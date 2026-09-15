/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.solace.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.Solace.UserPropertyValue;
import org.junit.Test;

public class SolaceRecordTest {

  @Test
  public void testDefaultPayloadType() {
    Record record = Record.builder().setMessageId("id").setPayload(new byte[0]).build();

    assertEquals(Record.PayloadType.BYTES_XML, record.getPayloadType());
  }

  @Test
  public void testDefaultUserPropertiesIsEmpty() {
    Record record = Record.builder().setMessageId("id").setPayload(new byte[0]).build();

    assertTrue(record.getUserPropertiesMap().isEmpty());
  }

  @Test
  public void testSetUserProperties() {
    Record record =
        Record.builder()
            .setMessageId("id")
            .setPayload(new byte[0])
            .setUserPropertiesMap(
                Collections.singletonMap("key", UserPropertyValue.stringValue("value")))
            .build();

    assertEquals(
        Collections.singletonMap("key", UserPropertyValue.stringValue("value")),
        record.getUserPropertiesMap());
  }

  @Test
  public void testUserPropertyBytesAreImmutable() {
    List<Byte> bytes = new java.util.ArrayList<>(Arrays.asList((byte) 1, (byte) 2, (byte) 3));
    UserPropertyValue value = UserPropertyValue.bytesValue(bytes);
    bytes.set(0, (byte) 4);

    assertEquals(Arrays.asList((byte) 1, (byte) 2, (byte) 3), value.getBytesValue());

    try {
      value.getBytesValue().set(1, (byte) 5);
      fail("Expected bytes value to be immutable.");
    } catch (UnsupportedOperationException expected) {
      // The copied binary representation cannot be mutated.
    }
    assertEquals(Arrays.asList((byte) 1, (byte) 2, (byte) 3), value.getBytesValue());
  }

  @Test
  public void testSetTextPayload() {
    Record record = Record.builder().setMessageId("id").setText("héllo").build();

    assertEquals(Record.PayloadType.TEXT, record.getPayloadType());
    assertArrayEquals("héllo".getBytes(StandardCharsets.UTF_8), record.getPayload());
    assertEquals("héllo", record.getText());
  }
}
