package com.viettel.filesync.model;

public class FileMetadata {
  private final String lastDirName;
  private final String lastFileName;

  public FileMetadata(String lastDirName, String lastFileName) {
    this.lastDirName = lastDirName;
    this.lastFileName = lastFileName;
  }

  public String getLastDirName() {
    return lastDirName;
  }

  public String getLastFileName() {
    return lastFileName;
  }
}
