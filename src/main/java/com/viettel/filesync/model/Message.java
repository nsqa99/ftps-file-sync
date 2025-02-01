package com.viettel.filesync.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
  @JsonProperty("path")
  private final String path;

  @JsonProperty("dateTimeString")
  private final String dateTime;

  public Message(String path, String dateTime) {
    this.path = path;
    this.dateTime = dateTime;
  }

  public String getPath() {
    return path;
  }

  public String getDateTime() {
    return dateTime;
  }
}
