package com.viettel.filesync.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class Message {
  private String path;
  private String dateTime;

  public Message(String path, String dateTime) {
    this.path = path;
    this.dateTime = dateTime;
  }

  public Message() {}

  @JsonProperty("path")
  public String getPath() {
    return path;
  }

  @JsonProperty("dateTimeString")
  public String getDateTime() {
    return dateTime;
  }
}
