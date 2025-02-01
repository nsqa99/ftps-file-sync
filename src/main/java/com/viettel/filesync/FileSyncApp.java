package com.viettel.filesync;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.viettel.filesync.kafka.MessageSerializer;
import com.viettel.filesync.model.FileInfo;
import com.viettel.filesync.model.FileMetadata;
import com.viettel.filesync.model.JobConfig;
import com.viettel.filesync.model.Message;
import java.io.*;
import java.nio.file.Files;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSyncApp {
  private static final Logger logger = LoggerFactory.getLogger(FileSyncApp.class);
  private static final String HDFS_SCHEME = "hdfs://vtcnamenode";
  private static final String HDFS_BASE_PATH = "/raw_zone/telecom/fake_bts/rawchr";
  private static final String DB_PATH = "jdbc:sqlite:ftps_metadata.db";
  private static final String KAFKA_TOPIC = "fakebts_log_sink";
  private static final String DATE_TIME_TEMPLATE = "%s %s:%s:00";
  private static final Pattern LS_DATA_PATTERN =
      Pattern.compile(".*\\s(\\w+\\s+\\d+\\s+\\d+:\\d+)\\s+(.+)");
  private static final DateTimeFormatter partitionFmt =
      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
  private static final DateTimeFormatter dateTimeFmt =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws IOException {
    String configFilePath = System.getProperty("config.file");
    Objects.requireNonNull(configFilePath);

    File configFile = new File(configFilePath);
    Config config = ConfigFactory.parseFile(configFile).resolve();
    JobConfig jobConfig =
        new JobConfig(
            config.getString("host"),
            config.getString("user"),
            config.getString("password"),
            config.getString("working-dir"),
            config.getString("server-name"),
            config.getInt("port"),
            config.getInt("tolerant-minutes"));

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
    Runnable task =
        () -> {
          KafkaProducer<String, Message> producer = null;
          try (FileSystem fs = createHdfsFs()) {
            createMetadataTable();

            logger.info("Start connecting to server");
            List<String> directoryLines =
                executeLftpCommand(
                    "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; ls "
                        + jobConfig.getWorkingDir()
                        + " | grep \"^d\";",
                    jobConfig);
            if (directoryLines.isEmpty()) {
              logger.info("No directories found.");
              return;
            }

            FileMetadata lastMetadata = getLatestMetadata();
            logger.info("[DEBUG] Latest metadata: " + lastMetadata);
            String lastDirName = null, lastFileName = null;
            if (lastMetadata != null) {
              lastDirName = lastMetadata.getLastDirName();
              lastFileName = lastMetadata.getLastFileName();
            }

            String latestDirectory =
                directoryLines.stream()
                    .map(FileSyncApp::extractFileName)
                    .filter(Objects::nonNull)
                    .max(String::compareTo)
                    .orElse(null);
            if (latestDirectory == null) return;
            logger.info("Latest directory: " + latestDirectory);

            producer = getKafkaProducer();

            logger.info("Connected to Kafka.");

            if (lastDirName == null || lastFileName == null) {
              // First time to run
              List<FileInfo> files = getFileListWithTimestamps(latestDirectory, jobConfig);
              if (files.isEmpty()) {
                logger.info("No files found in " + latestDirectory);
                return;
              }

              AtomicReference<String> newLatestFile = new AtomicReference<>("");

              KafkaProducer<String, Message> finalProducer = producer;
              files.stream()
                  .filter(
                      fi -> {
                        boolean notTooLate =
                            notTooLate(fi.getDateTime(), jobConfig.getTolerantMins());
                        if (!notTooLate) {
                          logger.info("[WARN] Skipping too old file: " + fi);
                        }

                        return notTooLate;
                      })
                  .forEach(
                      file -> {
                        String currFileNameValue = newLatestFile.get();
                        String fileName = file.getFileName();
                        if (currFileNameValue.isEmpty()) newLatestFile.set(fileName);
                        else {
                          if (currFileNameValue.compareTo(fileName) < 0) {
                            newLatestFile.set(fileName);
                          }
                        }

                        fetchLoadAndNotify(fs, latestDirectory, file, jobConfig, finalProducer);
                      });

              storeFileMetadata(latestDirectory, newLatestFile.get());
            } else {
              int compared = latestDirectory.compareTo(lastDirName);

              // greatest dir scanned < saved dir
              if (compared < 0) return;

              // greatest dir scanned > saved dir
              if (compared > 0) {
                // Check old dir if there are files not sync
                List<FileInfo> filesInOldDir = getFileListWithTimestamps(lastDirName, jobConfig);
                if (!filesInOldDir.isEmpty()) {
                  List<FileInfo> sortedOldDirFiles =
                      filesInOldDir.stream()
                          .sorted(Comparator.comparing(FileInfo::getFileName))
                          .collect(Collectors.toList());
                  Optional<FileInfo> mayLatestFile = sortedOldDirFiles.stream().findFirst();
                  if (mayLatestFile.isPresent()) {
                    FileInfo latestFile = mayLatestFile.get();
                    if (latestFile.getFileName().compareTo(lastFileName) > 0) {
                      List<String> sortedOldFileNames =
                          sortedOldDirFiles.stream()
                              .map(FileInfo::getFileName)
                              .collect(Collectors.toList());
                      // Scan and get remaining files in old directory.
                      int fileToStart = findClosestGE(sortedOldFileNames, lastFileName);

                      for (int idx = fileToStart; idx < sortedOldDirFiles.size(); idx++) {
                        FileInfo fileName = sortedOldDirFiles.get(idx);
                        fetchLoadAndNotify(fs, lastDirName, fileName, jobConfig, producer);
                      }
                    }
                  }
                }

                // Then fetch files in the latest directory
                List<FileInfo> files = getFileListWithTimestamps(latestDirectory, jobConfig);
                if (files.isEmpty()) {
                  logger.info("No files found in " + latestDirectory);
                  return;
                }

                AtomicReference<String> newLatestFile = new AtomicReference<>("");

                KafkaProducer<String, Message> finalProducer1 = producer;
                files.stream()
                    .filter(
                        fi -> {
                          boolean notTooLate =
                              notTooLate(fi.getDateTime(), jobConfig.getTolerantMins());
                          if (!notTooLate) {
                            logger.info("[WARN] Skipping too old file: " + fi);
                          }

                          return notTooLate;
                        })
                    .forEach(
                        file -> {
                          String fileName = file.getFileName();
                          String currFileNameValue = newLatestFile.get();
                          if (currFileNameValue.isEmpty()) newLatestFile.set(fileName);
                          else {
                            if (currFileNameValue.compareTo(fileName) < 0) {
                              newLatestFile.set(fileName);
                            }
                          }

                          fetchLoadAndNotify(fs, latestDirectory, file, jobConfig, finalProducer1);
                        });

                storeFileMetadata(latestDirectory, newLatestFile.get());
              } else {
                // stay in the same dir
                // only fetch file with file_name > saved file_name
                String finalLastFileName = lastFileName;
                AtomicReference<String> newLatestFile = new AtomicReference<>("");
                KafkaProducer<String, Message> finalProducer2 = producer;
                getFileListWithTimestamps(latestDirectory, jobConfig).stream()
                    .filter(f -> f != null && f.getFileName().compareTo(finalLastFileName) > 0)
                    .filter(
                        fi -> {
                          boolean notTooLate =
                              notTooLate(fi.getDateTime(), jobConfig.getTolerantMins());
                          if (!notTooLate) {
                            logger.info("[WARN] Skipping too old file: " + fi);
                          }

                          return notTooLate;
                        })
                    .peek(
                        validFile -> {
                          String fileName = validFile.getFileName();
                          if (newLatestFile.get().isEmpty()) {
                            newLatestFile.set(fileName);
                          } else {
                            if (fileName.compareTo(newLatestFile.get()) > 0) {
                              newLatestFile.set(fileName);
                            }
                          }
                        })
                    .forEach(
                        newFile ->
                            fetchLoadAndNotify(
                                fs, latestDirectory, newFile, jobConfig, finalProducer2));

                storeFileMetadata(lastDirName, newLatestFile.get());
              }
            }

            logger.info("Process completed successfully.");
          } catch (Exception e) {
            logger.error("Exception when processing data", e);
          } finally {
            if (producer != null) producer.close();
          }
        };

    // Schedule the task to run every 5 minutes
    long initialDelay = 0;
    long period = 5;
    TimeUnit unit = TimeUnit.MINUTES;

    scheduler.scheduleWithFixedDelay(task, initialDelay, period, unit);
  }

  private static void sendMessage(KafkaProducer<String, Message> producer, Message message) {
    // Send message
    ProducerRecord<String, Message> record = new ProducerRecord<>(KAFKA_TOPIC, null, message);

    try {
      producer.send(record);
      logger.info("Message sent successfully!");
    } catch (Exception e) {
      logger.error("Exception when sending Kafka message", e);
    }
  }

  private static KafkaProducer<String, Message> getKafkaProducer() {
    // Kafka properties
    Properties props = new Properties();
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        "10.79.85.136:9092,10.79.85.137:9092,10.79.85.138:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

    return new KafkaProducer<>(props);
  }

  private static void fetchLoadAndNotify(
      FileSystem fs,
      String dirName,
      FileInfo file,
      JobConfig jobConfig,
      KafkaProducer<String, Message> producer) {
    String fileName = file.getFileName();
    String partition = file.getPartition();
    String dateTime = file.getDateTime();
    String filePath = String.join("/", jobConfig.getWorkingDir(), dirName, fileName);

    logger.info(
        "Fetching file: "
            + fileName
            + ", path= '"
            + filePath
            + "', partition= '"
            + partition
            + "', dateTime= '"
            + dateTime
            + "'");
    try {
      String hdfsPath =
          String.join(
              "/",
              HDFS_BASE_PATH,
              "date_hour=" + partition,
              "server=" + jobConfig.getServerName(),
              fileName);
      Path hdfsWritePath = new Path(HDFS_SCHEME + hdfsPath);

      if (fs.exists(hdfsWritePath)) {
        logger.warn("[WARN] File existed: '" + HDFS_SCHEME + hdfsPath + "'");
        return;
      }

      byte[] content = getFileContent(filePath, jobConfig);
      boolean loaded = loadToHdfs(fs, content, hdfsWritePath);
      if (loaded) {
        logger.info("[FTPS] File " + fileName + " is loaded to HDFS");

        if (producer == null) {
          logger.warn("[Error] Found null producer, cannot send message to Kafka");
        } else {
          sendMessage(producer, new Message(hdfsPath, dateTime));
        }
      } else {
        logger.warn("[Error] Failed to load file into HDFS");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static boolean notTooLate(String dateTime, int tolerantMinutes) {
    LocalDateTime current = LocalDateTime.now();
    LocalDateTime inputTime = LocalDateTime.parse(dateTime, dateTimeFmt);

    LocalDateTime timeWithTolerant = inputTime.plusMinutes(tolerantMinutes);

    return timeWithTolerant.isEqual(current) || timeWithTolerant.isAfter(current);
  }

  public static int findClosestGE(List<String> list, String target) {
    int left = 0, right = list.size() - 1;
    int result = -1;

    while (left <= right) {
      int mid = left + (right - left) / 2;

      if (list.get(mid).compareTo(target) >= 0) {
        result = mid; // Possible answer
        right = mid - 1; // Try to find a smaller >= element
      } else {
        left = mid + 1; // Ignore left half
      }
    }
    return result; // Returns the closest >= value or null if none exists
  }
  /** Executes an LFTP command and returns the output as a List */
  private static List<String> executeLftpCommand(String command, JobConfig jobConfig)
      throws Exception {
    List<String> result = new ArrayList<>();
    ProcessBuilder pb =
        new ProcessBuilder(
            "bash",
            "-c",
            "lftp -u "
                + jobConfig.getUsername()
                + ","
                + jobConfig.getPassword()
                + " -e \""
                + command
                + "; bye\" "
                + jobConfig.getHost()
                + ":"
                + jobConfig.getPort());
    Process process = pb.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    String line;
    while ((line = reader.readLine()) != null) {
      result.add(line.trim());
    }
    process.waitFor();
    return result;
  }

  private static String extractFileName(String raw) {
    Matcher matcher = LS_DATA_PATTERN.matcher(raw);
    if (matcher.find()) return matcher.group(2);

    return null;
  }

  /** Retrieves file list along with last modified timestamps */
  private static List<FileInfo> getFileListWithTimestamps(String directory, JobConfig jobConfig)
      throws Exception {
    logger.info("Start listing file in directory: '" + directory + "'");
    String listCommand =
        "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; ls "
            + jobConfig.getWorkingDir()
            + "/"
            + directory
            + " | grep -v \"^d\";";
    List<String> output = executeLftpCommand(listCommand, jobConfig);
    List<FileInfo> files = new ArrayList<>();

    for (String line : output) {
      logger.info("[DEBUG] Processing line: " + line);
      Matcher matcher = LS_DATA_PATTERN.matcher(line);
      if (matcher.find()) {
        String dateString = matcher.group(1);
        String fileName = matcher.group(2);
        Map<String, Integer> dateHourAndMinute = parseDateAndHour(dateString);

        files.add(
            new FileInfo(
                fileName,
                directoryToPartition(directory, dateHourAndMinute.get("hour")),
                directoryToDateTime(
                    directory, dateHourAndMinute.get("hour"), dateHourAndMinute.get("minute"))));
      } else logger.warn("[Error] Skipping line not match pattern: " + line);
    }

    return files;
  }

  public static Map<String, Integer> parseDateAndHour(String input) {
    String[] parts = input.trim().split("\\s+");

    int day = Integer.parseInt(parts[1]);
    String[] timeParts = parts[2].split(":");
    int hour = Integer.parseInt(timeParts[0]);
    int minute = Integer.parseInt(timeParts[1]);

    Map<String, Integer> result = new HashMap<>();
    result.put("day", day);
    result.put("hour", hour);
    result.put("minute", minute);

    return result;
  }

  private static String directoryToPartition(String dirName, int hour) {
    return String.join(
        "-",
        dirName.substring(0, 4),
        dirName.substring(4, 6),
        dirName.substring(6, 8),
        String.format("%02d", hour));
  }

  private static String directoryToDateTime(String dirName, int hour, int minute) {
    return String.format(
        DATE_TIME_TEMPLATE,
        String.join("-", dirName.substring(0, 4), dirName.substring(4, 6), dirName.substring(6, 8)),
        String.format("%02d", hour),
        String.format("%02d", minute));
  }

  /** Stores file metadata (filename + last_modified_time) in SQLite */
  private static void storeFileMetadata(String directory, String file) throws Exception {
    try (Connection conn = DriverManager.getConnection(DB_PATH)) {
      PreparedStatement ps =
          conn.prepareStatement("INSERT INTO file_metadata (directory, file_name) VALUES (?, ?)");
      ps.setString(1, directory);
      ps.setString(2, file);
      ps.executeUpdate();
    }
  }

  private static FileMetadata getLatestMetadata() throws Exception {
    String query =
        "select directory\n"
            + ", file_name\n"
            + "from file_metadata\n"
            + "order by created_time desc\n"
            + "limit 1";
    try (Connection conn = DriverManager.getConnection(DB_PATH);
        PreparedStatement ps = conn.prepareStatement(query)) {
      ResultSet rs = ps.executeQuery();

      if (rs.next()) {
        String dirName = rs.getString("directory");
        String fileName = rs.getString("file_name");

        return new FileMetadata(dirName, fileName);
      }

      return null;
    }
  }

  private static void createMetadataTable() throws Exception {
    try (Connection conn = DriverManager.getConnection(DB_PATH)) {
      Statement stmt = conn.createStatement();
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS file_metadata (directory TEXT, file_name TEXT, created_time DATETIME DEFAULT CURRENT_TIMESTAMP)");
    }
  }

  private static long getEpochMillisFromDateTime(LocalDateTime ldt) {
    return ldt.atZone(ZoneId.of("Asia/Ho_Chi_Minh")).toInstant().toEpochMilli();
  }

  /** Reads file content from FTP */
  private static byte[] getFileContent(String filePath, JobConfig jobConfig) throws Exception {
    String fetchCommand =
        "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; cat "
            + filePath
            + "; bye";
    logger.info("[DEBUG] Fetch command: '" + fetchCommand + "'");
    ProcessBuilder pb =
        new ProcessBuilder(
            "bash",
            "-c",
            "lftp -u "
                + jobConfig.getUsername()
                + ","
                + jobConfig.getPassword()
                + " -e \""
                + fetchCommand
                + "\" "
                + jobConfig.getHost()
                + ":"
                + jobConfig.getPort());
    Process process = pb.start();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteArrayOutputStream errorStream = new ByteArrayOutputStream();

    // Consume input stream and error stream
    InputStream inputStream = process.getInputStream();
    InputStream processErrorStream = process.getErrorStream();

    Thread inputStreamThread =
        new Thread(
            () -> {
              try {
                IOUtils.copy(inputStream, outputStream);
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    Thread errorStreamThread =
        new Thread(
            () -> {
              try {
                IOUtils.copy(processErrorStream, errorStream);
              } catch (Exception e) {
                e.printStackTrace();
              }
            });

    inputStreamThread.start();
    errorStreamThread.start();

    inputStreamThread.join();
    errorStreamThread.join();

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("Error occurred: " + errorStream);
    }

    return outputStream.toByteArray();
  }

  private static byte[] getAndLoadFileContent(String filePath, JobConfig jobConfig)
      throws Exception {
    String fetchCommand =
        "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; get -O /u01/vbi_app/fake_bts/ftps-file-sync/tmp/ "
            + filePath
            + ";";
    logger.info("[DEBUG] Fetch command: '" + fetchCommand + "'");
    ProcessBuilder pb =
        new ProcessBuilder(
            "bash",
            "-c",
            "lftp -u "
                + jobConfig.getUsername()
                + ","
                + jobConfig.getPassword()
                + " -e \""
                + fetchCommand
                + "bye\" "
                + jobConfig.getHost()
                + ":"
                + jobConfig.getPort());
    Process process = pb.start();

    process.waitFor();

    if (process.exitValue() != 0) {
      throw new RuntimeException("Failed to fetch the file. Exit code: " + process.exitValue());
    }

    // Read file content from /tmp folder
    String fileName = new File(filePath).getName();
    File downloadedFile = new File("/u01/vbi_app/fake_bts/ftps-file-sync/tmp/" + fileName);

    if (!downloadedFile.exists()) {
      throw new RuntimeException(
          "Downloaded file does not exist: " + downloadedFile.getAbsolutePath());
    }

    byte[] fileContent;
    try (InputStream fileInputStream = Files.newInputStream(downloadedFile.toPath())) {
      fileContent = IOUtils.toByteArray(fileInputStream);
    }

    // Delete the file after processing
    if (!downloadedFile.delete()) {
      logger.warn("Warning: Failed to delete the file: " + downloadedFile.getAbsolutePath());
    }

    return fileContent;
  }

  /** Saves content to HDFS */
  private static boolean loadToHdfs(FileSystem fs, byte[] content, Path hdfsWritePath)
      throws Exception {
    logger.info("File will be load into HDFS with path = '" + hdfsWritePath + "'");

    try (FSDataOutputStream os = fs.create(hdfsWritePath)) {
      os.write(content);
    }
    boolean fileExists = fs.exists(hdfsWritePath);
    if (fileExists) fs.setOwner(hdfsWritePath, "vbi_app", "vbi_app");

    return fileExists;
  }

  private static FileSystem createHdfsFs() throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/3.1.4.0-315/0/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/3.1.4.0-315/0/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    return FileSystem.get(conf);
  }
}
