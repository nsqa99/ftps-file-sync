package com.viettel.filesync;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.viettel.filesync.model.Message;

public class MessageSerializerTest {

  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Message m = new Message("x", "y");

    System.out.println(objectMapper.writeValueAsString(m));
  }
}
