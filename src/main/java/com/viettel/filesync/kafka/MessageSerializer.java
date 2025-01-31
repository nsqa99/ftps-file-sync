package com.viettel.filesync.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.viettel.filesync.model.Message;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerializer implements Serializer<Message> {
  private final ObjectMapper objectMapper = new ObjectMapper();
  @Override
  public byte[] serialize(String s, Message message) {
    try {
      return objectMapper.writeValueAsBytes(message);
    } catch (Exception e) {
      throw new RuntimeException("Error serializing message", e);
    }
  }
}
