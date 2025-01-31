package com.viettel.filesync.model;

public class FileInfo {
  private final String fileName;
  private final String partition;
  private final String dateTime;

  public FileInfo(String fileName, String partition, String dateTime) {
    this.fileName = fileName;
    this.partition = partition;
    this.dateTime = dateTime;
  }

  public String getFileName() {
    return fileName;
  }

  public String getPartition() {
    return partition;
  }

  public String getDateTime() {
    return dateTime;
  }
}
