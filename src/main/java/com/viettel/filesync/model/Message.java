package com.viettel.filesync.model;

import org.codehaus.jackson.annotate.JsonProperty;

public class Message {
  private String path;
  private String partition;
  private String fileName;
  private String dateTime;

  public Message(String path, String partition, String fileName, String dateTime) {
    this.path = path;
    this.partition = partition;
    this.fileName = fileName;
    this.dateTime = dateTime;
  }

  public Message() {}

  @JsonProperty("path")
  public String getPath() {
    return path;
  }

  @JsonProperty("partition")
  public String getPartition() {
    return partition;
  }

  @JsonProperty("fileName")
  public String getFileName() {
    return fileName;
  }

  @JsonProperty("dateTime")
  public String getDateTime() {
    return dateTime;
  }
}
