package com.viettel.filesync.model;

public class JobConfig {
  private final String host;
  private final String username;
  private final String password;
  private final String workingDir;
  private final String serverName;
  private final int port;
  private final int tolerantMins;

  public JobConfig(
      String host,
      String username,
      String password,
      String workingDir,
      String serverName,
      int port,
      int tolerantMins) {
    this.host = host;
    this.username = username;
    this.password = password;
    this.workingDir = workingDir;
    this.serverName = serverName;
    this.port = port;
    this.tolerantMins = tolerantMins;
  }

  public String getHost() {
    return host;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getWorkingDir() {
    return workingDir;
  }

  public int getPort() {
    return port;
  }

  public String getServerName() {
    return serverName;
  }

  public int getTolerantMins() {
    return tolerantMins;
  }
}
