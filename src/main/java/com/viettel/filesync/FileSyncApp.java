package com.viettel.filesync;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.viettel.filesync.kafka.MessageSerializer;
import com.viettel.filesync.model.FTPConfig;
import com.viettel.filesync.model.FileInfo;
import com.viettel.filesync.model.FileMetadata;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.viettel.filesync.model.Message;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class FileSyncApp {
  private static final String HDFS_BASE_PATH = "hdfs://vtc-namenode/raw_zone/telecom/CHR";
  private static final String DB_PATH = "jdbc:sqlite:ftp_metadata.db";
  private static final String KAFKA_TOPIC = "topic_name";
  private static final Pattern LS_DATA_PATTERN =
      Pattern.compile(".*\\s(\\w+\\s+\\d+\\s+\\d+:\\d+)\\s+(.+)");
  private static final DateTimeFormatter partitionFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
  private static final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) {
    String configFilePath = System.getProperty("config.file");
    Objects.requireNonNull(configFilePath);

    File configFile = new File(configFilePath);
    Config config = ConfigFactory.parseFile(configFile).resolve();
    FTPConfig ftpConfig =
        new FTPConfig(
            config.getString("host"),
            config.getString("user"),
            config.getString("password"),
            config.getString("working-dir"),
            config.getString("server-name"),
            config.getInt("port"));
    KafkaProducer<String, Message> producer = null;

    try {
      createMetadataTable();

      System.out.println("Start connecting to server");
      List<String> directoryLines =
          executeLftpCommand(
              "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; ls "
                  + ftpConfig.getWorkingDir()
                  + " | grep \"^d\";",
              ftpConfig);
      if (directoryLines.isEmpty()) {
        System.out.println("No directories found.");
        return;
      }

      FileMetadata lastMetadata = getLatestMetadata();
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
      System.out.println("Latest directory: " + latestDirectory);

      producer = getKafkaProducer();

      if (lastDirName == null || lastFileName == null) {
        // First time to run
        List<FileInfo> files = getFileListWithTimestamps(latestDirectory, ftpConfig);
        if (files.isEmpty()) {
          System.out.println("No files found in " + latestDirectory);
          return;
        }

        AtomicReference<String> newLatestFile = new AtomicReference<>("");

        KafkaProducer<String, Message> finalProducer = producer;
        files.forEach(
            file -> {
              String currFileNameValue = newLatestFile.get();
              String fileName = file.getFileName();
              if (currFileNameValue.isEmpty()) newLatestFile.set(fileName);
              else {
                if (currFileNameValue.compareTo(fileName) < 0) {
                  newLatestFile.set(fileName);
                }
              }

              fetchLoadAndNotify(latestDirectory, file, ftpConfig, finalProducer);
            });

        storeFileMetadata(latestDirectory, newLatestFile.get());
      } else {
        int compared = latestDirectory.compareTo(lastDirName);

        // greatest dir scanned < saved dir
        if (compared < 0) return;

        // greatest dir scanned > saved dir
        if (compared > 0) {
          // Check old dir if there are files not sync
          List<FileInfo> filesInOldDir = getFileListWithTimestamps(lastDirName, ftpConfig);
          if (!filesInOldDir.isEmpty()) {
            List<FileInfo> sortedOldDirFiles =
                filesInOldDir.stream().sorted(Comparator.comparing(FileInfo::getFileName)).collect(Collectors.toList());
            Optional<FileInfo> mayLatestFile = sortedOldDirFiles.stream().findFirst();
            if (mayLatestFile.isPresent()) {
              FileInfo latestFile = mayLatestFile.get();
              if (latestFile.getFileName().compareTo(lastFileName) > 0) {
                List<String> sortedOldFileNames = sortedOldDirFiles.stream().map(FileInfo::getFileName).collect(Collectors.toList());
                // Scan and get remaining files in old directory.
                int fileToStart = findClosestGE(sortedOldFileNames, lastFileName);

                for (int idx = fileToStart; idx < sortedOldDirFiles.size(); idx++) {
                  FileInfo fileName = sortedOldDirFiles.get(idx);
                  fetchLoadAndNotify(lastDirName, fileName, ftpConfig, producer);
                }
              }
            }
          }

          // Then fetch files in the latest directory
          List<FileInfo> files = getFileListWithTimestamps(latestDirectory, ftpConfig);
          if (files.isEmpty()) {
            System.out.println("No files found in " + latestDirectory);
            return;
          }

          AtomicReference<String> newLatestFile = new AtomicReference<>("");

          KafkaProducer<String, Message> finalProducer1 = producer;
          files.forEach(
              file -> {
                String fileName = file.getFileName();
                String currFileNameValue = newLatestFile.get();
                if (currFileNameValue.isEmpty()) newLatestFile.set(fileName);
                else {
                  if (currFileNameValue.compareTo(fileName) < 0) {
                    newLatestFile.set(fileName);
                  }
                }

                fetchLoadAndNotify(latestDirectory, file, ftpConfig, finalProducer1);
              });

          storeFileMetadata(latestDirectory, newLatestFile.get());
        } else {
          // stay in the same dir
          // only fetch file with file_name > saved file_name
          String finalLastFileName = lastFileName;
          AtomicReference<String> newLatestFile = new AtomicReference<>("");
          KafkaProducer<String, Message> finalProducer2 = producer;
          getFileListWithTimestamps(latestDirectory, ftpConfig).stream()
              .filter(f -> f != null && f.getFileName().compareTo(finalLastFileName) > 0)
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
              .forEach(newFile -> fetchLoadAndNotify(latestDirectory, newFile, ftpConfig, finalProducer2));

          storeFileMetadata(lastDirName, newLatestFile.get());
        }
      }

      System.out.println("Process completed successfully.");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (producer != null) producer.close();
    }
  }

  private static void sendMessage(KafkaProducer<String, Message> producer, Message message) {
    // Send message
    ProducerRecord<String, Message> record = new ProducerRecord<>(KAFKA_TOPIC, null, message);

    try {
      producer.send(record);
      System.out.println("Message sent successfully!");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  private static KafkaProducer<String, Message> getKafkaProducer() {
    // Kafka properties
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker1:9092,kafka-broker2:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

    return new KafkaProducer<>(props);
  }
  private static void fetchLoadAndNotify(String dirName, FileInfo file, FTPConfig ftpConfig,  KafkaProducer<String, Message> producer) {
    String fileName = file.getFileName();
    String partition = file.getPartition();
    String dateTime = file.getDateTime();
    String filePath = String.join("/", ftpConfig.getWorkingDir(), dirName, fileName);
    try {
      byte[] content = getFileContent(filePath, ftpConfig);
      String hdfsPath = String.join("/", "server=" + ftpConfig.getServerName(), "partition=" + partition, fileName);
      boolean loaded = loadToHdfs(content, hdfsPath);
      if (loaded) {
        System.out.println("[FTPS] File " + fileName + " is loaded to HDFS");

        if (producer == null) {
          System.out.println("[Error] Found null producer, cannot send message to Kafka");
        } else {
          sendMessage(producer, new Message(hdfsPath, partition, fileName, dateTime));
        }
      } else {
        System.out.println("[Error] Failed to load file into HDFS");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
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
  private static List<String> executeLftpCommand(String command, FTPConfig ftpConfig)
      throws Exception {
    List<String> result = new ArrayList<>();
    ProcessBuilder pb =
        new ProcessBuilder(
            "bash",
            "-c",
            "lftp -u "
                + ftpConfig.getUsername()
                + ","
                + ftpConfig.getPassword()
                + " -e \""
                + command
                + "; bye\" "
                + ftpConfig.getHost()
                + ":"
                + ftpConfig.getPort());
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
  private static List<FileInfo> getFileListWithTimestamps(String directory, FTPConfig ftpConfig)
      throws Exception {
    List<String> output =
        executeLftpCommand(
            "set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; ls "
                + ftpConfig.getWorkingDir() + "/" + directory
                + " | grep -v \"^d\";",
            ftpConfig);
    List<FileInfo> files = new ArrayList<>();

    DateTimeFormatter df = DateTimeFormatter.ofPattern("MMM dd HH:mm");
    Calendar calendar = Calendar.getInstance();
    int currentYear = calendar.get(Calendar.YEAR);

    for (String line : output) {
      Matcher matcher = LS_DATA_PATTERN.matcher(line);
      if (matcher.find()) {
        String dateString = matcher.group(1);
        String fileName = matcher.group(2);
        LocalDateTime date = LocalDateTime.parse(dateString, df);
        LocalDateTime dateWithCurrentYear = date.withYear(currentYear).withSecond(0).withNano(0);
        LocalDateTime currentDate = LocalDateTime.now();

        LocalDateTime finalDate = dateWithCurrentYear.isAfter(currentDate) ?
            dateWithCurrentYear.withYear(currentYear - 1) : dateWithCurrentYear;


        files.add(new FileInfo(fileName, partitionFmt.format(finalDate), dateTimeFmt.format(finalDate)));
      }
    }

    return files;
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
            + ", filename\n"
            + "from file_metadata\n"
            + "order by created_time desc\n"
            + "limit 1";
    try (Connection conn = DriverManager.getConnection(DB_PATH);
        PreparedStatement ps = conn.prepareStatement(query)) {
      ResultSet rs = ps.executeQuery();

      if (rs.next()) {
        String dirName = rs.getString("directory");
        String fileName = rs.getString("filename");

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
  private static byte[] getFileContent(String filePath, FTPConfig ftpConfig) throws Exception {
    ProcessBuilder pb =
        new ProcessBuilder(
            "bash",
            "-c",
            "lftp -u "
                + ftpConfig.getUsername()
                + ","
                + ftpConfig.getPassword()
                + " -e \"set ftp:ssl-force true;set ftp:ssl-protect-data true;set ssl:verify-certificate false; cat "
                + filePath
                + "; bye\" "
                + ftpConfig.getHost()
                + ":"
                + ftpConfig.getPort());
    Process process = pb.start();
    InputStream inputStream = process.getInputStream();
    if (inputStream == null) {
      throw new RuntimeException("Found null input stream when process fetching file content");
    }

    process.waitFor();

    return IOUtils.toByteArray(inputStream);
  }

  /** Saves content to HDFS */
  // TODO: make sure core-site.xml and hdfs-site.xml is configured correctly
  private static boolean loadToHdfs(byte[] content, String fileName) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path hdfsWritePath = new Path(String.join("/", HDFS_BASE_PATH, fileName));

    try (FSDataOutputStream os = fs.create(hdfsWritePath)) {
      os.write(content);
    }

    return fs.exists(hdfsWritePath);
  }
}
