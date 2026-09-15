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
package org.apache.beam.sdk.io.solace.broker;

import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.Topic;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class SolaceUserPropertiesMapper {

  private SolaceUserPropertiesMapper() {}

  public static Map<String, Solace.UserPropertyValue> toUserPropertyValueMap(
      @Nullable SDTMap properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Solace.UserPropertyValue> userProperties = new HashMap<>();
    for (String key : properties.keySet()) {
      try {
        Object value = properties.get(key);
        if (value != null) {
          userProperties.put(key, toUserPropertyValue(value));
        }
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return Collections.unmodifiableMap(userProperties);
  }

  public static List<Solace.UserProperty> toUserProperties(
      Map<String, Solace.UserPropertyValue> properties) {
    List<Solace.UserProperty> userProperties = new ArrayList<>();
    for (Map.Entry<String, Solace.UserPropertyValue> entry : properties.entrySet()) {
      Solace.UserPropertyValue propertyValue = entry.getValue();
      userProperties.add(
          Solace.UserProperty.builder()
              .setKey(entry.getKey())
              .setKind(propertyValue.getKind())
              .setValue(encode(propertyValue))
              .build());
    }
    return Collections.unmodifiableList(userProperties);
  }

  public static Map<String, Solace.UserPropertyValue> toUserPropertyValueMap(
      List<Solace.UserProperty> properties) {
    Map<String, Solace.UserPropertyValue> userProperties = new HashMap<>();
    for (Solace.UserProperty property : properties) {
      Solace.UserPropertyValue propertyValue = decode(property.getValue());
      if (propertyValue.getKind() != property.getKind()) {
        throw new IllegalArgumentException(
            "Encoded user property '"
                + property.getKey()
                + "' has kind "
                + propertyValue.getKind()
                + ", but its entry declares "
                + property.getKind()
                + ".");
      }
      userProperties.put(property.getKey(), propertyValue);
    }
    return Collections.unmodifiableMap(userProperties);
  }

  public static SDTMap toSDTMap(Map<String, Solace.UserPropertyValue> properties) {
    SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
    for (Map.Entry<String, Solace.UserPropertyValue> entry : properties.entrySet()) {
      try {
        putUserProperty(sdtMap, entry.getKey(), entry.getValue());
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return sdtMap;
  }

  private static Solace.UserPropertyValue toUserPropertyValue(@NonNull Object value) {
    if (value instanceof Boolean) {
      return Solace.UserPropertyValue.booleanValue((Boolean) value);
    }
    if (value instanceof Byte) {
      return Solace.UserPropertyValue.byteValue((Byte) value);
    }
    if (value instanceof Short) {
      return Solace.UserPropertyValue.shortValue((Short) value);
    }
    if (value instanceof Integer) {
      return Solace.UserPropertyValue.integerValue((Integer) value);
    }
    if (value instanceof Long) {
      return Solace.UserPropertyValue.longValue((Long) value);
    }
    if (value instanceof Float) {
      return Solace.UserPropertyValue.floatValue((Float) value);
    }
    if (value instanceof Double) {
      return Solace.UserPropertyValue.doubleValue((Double) value);
    }
    if (value instanceof Character) {
      return Solace.UserPropertyValue.characterValue((Character) value);
    }
    if (value instanceof String) {
      return Solace.UserPropertyValue.stringValue((String) value);
    }
    if (value instanceof byte[]) {
      return Solace.UserPropertyValue.bytesValue(Bytes.asList((byte[]) value));
    }
    if (value instanceof ByteArray) {
      return Solace.UserPropertyValue.bytesValue(Bytes.asList(((ByteArray) value).asBytes()));
    }

    // destination
    if (value instanceof Destination) {
      return Solace.UserPropertyValue.destinationValue(toSolaceDestination((Destination) value));
    }

    // sdt map
    if (value instanceof SDTMap) {
      return Solace.UserPropertyValue.mapValue(toUserPropertyValueMap((SDTMap) value));
    }

    // sdt stream
    if (value instanceof SDTStream) {
      return Solace.UserPropertyValue.streamValue(toUserPropertyValueList((SDTStream) value));
    }

    throw new IllegalArgumentException("Unsupported user property value type: " + value.getClass());
  }

  private static List<Solace.UserPropertyValue> toUserPropertyValueList(@NonNull SDTStream stream) {
    List<Solace.UserPropertyValue> items = new ArrayList<>();
    stream.rewind();
    while (stream.hasRemaining()) {
      try {
        items.add(toUserPropertyValue(stream.read()));
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return items;
  }

  private static SDTStream toSDTStream(@Nullable List<Solace.UserPropertyValue> values) {
    SDTStream stream = JCSMPFactory.onlyInstance().createStream();

    if (values == null) {
      return stream;
    }

    for (Solace.UserPropertyValue userPropertyValue : values) {
      try {
        writeStream(stream, userPropertyValue);
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return stream;
  }

  private static Solace.Destination toSolaceDestination(Destination destination) {
    return Solace.Destination.builder()
        .setType(
            destination instanceof Topic
                ? Solace.DestinationType.TOPIC
                : Solace.DestinationType.QUEUE)
        .setName(destination.getName())
        .build();
  }

  private static Destination toDestination(Solace.Destination destination) {
    if (destination.getType() == Solace.DestinationType.QUEUE) {
      return JCSMPFactory.onlyInstance().createQueue(destination.getName());
    }
    return JCSMPFactory.onlyInstance().createTopic(destination.getName());
  }

  private static void putUserProperty(
      SDTMap map, String key, Solace.UserPropertyValue propertyValue) throws SDTException {
    if (propertyValue == null) {
      return;
    }
    switch (propertyValue.getKind()) {
      case BOOLEAN:
        ifNotNull(propertyValue.getBooleanValue(), v -> map.putBoolean(key, v));
        return;
      case BYTE:
        ifNotNull(propertyValue.getByteValue(), v -> map.putByte(key, v));
        return;
      case SHORT:
        ifNotNull(propertyValue.getShortValue(), v -> map.putShort(key, v));
        return;
      case INTEGER:
        ifNotNull(propertyValue.getIntegerValue(), v -> map.putInteger(key, v));
        return;
      case LONG:
        ifNotNull(propertyValue.getLongValue(), v -> map.putLong(key, v));
        return;
      case FLOAT:
        ifNotNull(propertyValue.getFloatValue(), v -> map.putFloat(key, v));
        return;
      case DOUBLE:
        ifNotNull(propertyValue.getDoubleValue(), v -> map.putDouble(key, v));
        return;
      case CHARACTER:
        ifNotNull(propertyValue.getCharacterValue(), v -> map.putCharacter(key, v));
        return;
      case STRING:
        ifNotNull(propertyValue.getStringValue(), v -> map.putString(key, v));
        return;
      case BYTES:
        ifNotNull(propertyValue.getBytesValue(), v -> map.putBytes(key, Bytes.toArray(v)));
        return;
      case DESTINATION:
        ifNotNull(
            propertyValue.getDestinationValue(), v -> map.putDestination(key, toDestination(v)));
        return;
      case MAP:
        ifNotNull(propertyValue.getMapValue(), v -> map.putMap(key, toNestedSDTMap(v)));
        return;
      case STREAM:
        ifNotNull(propertyValue.getStreamValue(), v -> map.putStream(key, toSDTStream(v)));
        return;
      default:
        throw new IllegalArgumentException(
            "Unsupported user property at '" + key + "': " + propertyValue.getKind());
    }
  }

  private static void writeStream(SDTStream stream, Solace.UserPropertyValue propertyValue)
      throws SDTException {
    if (propertyValue == null) {
      return;
    }

    switch (propertyValue.getKind()) {
      case BOOLEAN:
        ifNotNull(propertyValue.getBooleanValue(), stream::writeBoolean);
        return;
      case BYTE:
        ifNotNull(propertyValue.getByteValue(), stream::writeByte);
        return;
      case SHORT:
        ifNotNull(propertyValue.getShortValue(), stream::writeShort);
        return;
      case INTEGER:
        ifNotNull(propertyValue.getIntegerValue(), stream::writeInteger);
        return;
      case LONG:
        ifNotNull(propertyValue.getLongValue(), stream::writeLong);
        return;
      case FLOAT:
        ifNotNull(propertyValue.getFloatValue(), stream::writeFloat);
        return;
      case DOUBLE:
        ifNotNull(propertyValue.getDoubleValue(), stream::writeDouble);
        return;
      case CHARACTER:
        ifNotNull(propertyValue.getCharacterValue(), stream::writeCharacter);
        return;
      case STRING:
        ifNotNull(propertyValue.getStringValue(), stream::writeString);
        return;
      case BYTES:
        ifNotNull(propertyValue.getBytesValue(), v -> stream.writeBytes(Bytes.toArray(v)));
        return;
      case DESTINATION:
        ifNotNull(
            propertyValue.getDestinationValue(), v -> stream.writeDestination(toDestination(v)));
        return;
      case MAP:
        ifNotNull(propertyValue.getMapValue(), v -> stream.writeMap(toNestedSDTMap(v)));
        return;
      case STREAM:
        ifNotNull(propertyValue.getStreamValue(), v -> stream.writeStream(toSDTStream(v)));
        return;
      default:
        throw new IllegalArgumentException("Unsupported user property: " + propertyValue.getKind());
    }
  }

  @FunctionalInterface
  private interface SDTConsumer<T> {
    void accept(@NonNull T value) throws SDTException;
  }

  private static <T> void ifNotNull(@Nullable T value, SDTConsumer<T> consumer)
      throws SDTException {
    if (value != null) {
      consumer.accept(value);
    }
  }

  private static SDTMap toNestedSDTMap(Map<String, Solace.UserPropertyValue> properties) {
    SDTMap sdtMap = JCSMPFactory.onlyInstance().createMap();
    for (Map.Entry<String, Solace.UserPropertyValue> entry : properties.entrySet()) {
      try {
        putUserProperty(sdtMap, entry.getKey(), entry.getValue());
      } catch (SDTException e) {
        throw new RuntimeException(e);
      }
    }
    return sdtMap;
  }


  private static byte[] encode(Solace.UserPropertyValue value) {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
         ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
      objectOutput.writeObject(value);
      return output.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not encode user property value.", e);
    }
  }

  private static Solace.UserPropertyValue decode(byte[] value) {
    try (ObjectInputStream objectInput = new ObjectInputStream(new ByteArrayInputStream(value))) {
      Object decoded = objectInput.readObject();
      if (!(decoded instanceof Solace.UserPropertyValue)) {
        throw new IllegalArgumentException(
                "Encoded user property does not contain a UserPropertyValue.");
      }
      return (Solace.UserPropertyValue) decoded;
    } catch (IOException | ClassNotFoundException e) {
      throw new IllegalArgumentException("Could not decode user property value.", e);
    }
  }
}
