import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.Locale;

public class SummarizeWorker {
  
  private final S3Client s3Client;
  private final String outputBucket;
  
  private static final DateTimeFormatter INPUT_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("dd/MM/yyyy hh:mm:ss a")
    .toFormatter(Locale.ENGLISH);
  
  public SummarizeWorker(String outputBucket) {
    this.s3Client = S3Client.builder().build();
    this.outputBucket = outputBucket;
  }
  
  public String run(String sourceBucket, String sourceKey) {
    try {
      try {
        s3Client.headObject(HeadObjectRequest.builder()
          .bucket(sourceBucket)
          .key(sourceKey)
          .build());
      } catch (NoSuchKeyException e) {
        throw new RuntimeException("Object " + sourceKey + " does not exist in bucket " + sourceBucket);
      }
      
      System.out.println("Attempting to access bucket: " + sourceBucket);
      System.out.println("Attempting to access key: " + sourceKey);
      
      ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(GetObjectRequest.builder()
        .bucket(sourceBucket)
        .key(sourceKey)
        .build());
      
      String processedCsv = processCsv(s3ObjectResponse);
      
      String outputKey = "daily_summary_" + java.time.LocalDate.now() + "_" + sourceKey;
      
      uploadToS3(processedCsv, outputKey);
      
      return "Successfully processed " + sourceKey;
      
    } catch (Exception e) {
      throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
    }
  }
  
  private void uploadToS3(String content, String key) throws IOException {
    byte[] contentBytes = content.getBytes();
    try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
      PutObjectRequest putRequest = PutObjectRequest.builder()
        .bucket(outputBucket)
        .key(key)
        .contentType("text/csv")
        .build();
      
      s3Client.putObject(putRequest, RequestBody.fromBytes(contentBytes));
    }
  }
  
  public String processCsv(InputStream inputStream) throws IOException, CsvException {
    Map<String, SummarizeWorker.AggregatedData> dailyTraffic = new HashMap<>();
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
         CSVReader csvReader = new CSVReader(reader)) {
      
      List<String[]> records = csvReader.readAll();
      
      for (int i = 1; i < records.size(); i++) {
        String[] record = records.get(i);
        if (record.length >= 9) {
          processRecord(record, dailyTraffic);
        }
      }
      
      return convertToOutput(dailyTraffic);
    }
  }
  
  private void processRecord(String[] record, Map<String, SummarizeWorker.AggregatedData> dailyTraffic) {
    try {
      String timestampStr = record[6].trim();
      if (timestampStr.isEmpty()) {
        return;
      }
      
      LocalDateTime timestamp = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);
      String date = timestamp.toLocalDate().toString();
      
      String sourceIp = record[1].trim();
      String destIp = record[3].trim();
      
      if (sourceIp.isEmpty() || destIp.isEmpty()) {
        return;
      }
      
      String key = String.format("%s,%s,%s", date, sourceIp, destIp);
      
      long flowDuration = parseLongSafely(record[7]);
      long forwardPackets = parseLongSafely(record[8]);
      
      dailyTraffic.computeIfAbsent(key, k -> new SummarizeWorker.AggregatedData())
        .addData(flowDuration, forwardPackets);
      
    } catch (Exception e) {
      System.err.println("Error processing record: " + Arrays.toString(record));
      System.err.println("Error details: " + e.getMessage());
    }
  }
  
  private long parseLongSafely(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException | NullPointerException e) {
      return 0L;
    }
  }
  
  private String convertToOutput(Map<String, SummarizeWorker.AggregatedData> dailyTraffic) {
    StringBuilder output = new StringBuilder();
    output.append("date,source_ip,destination_ip,total_flow_duration,total_forward_packets\n");
    
    dailyTraffic.entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .forEach(entry -> {
        output.append(entry.getKey())
          .append(",")
          .append(entry.getValue().totalDuration)
          .append(",")
          .append(entry.getValue().totalPackets)
          .append("\n");
      });
    
    return output.toString();
  }
  
  static class AggregatedData {
    long totalDuration = 0;
    long totalPackets = 0;
    
    void addData(long duration, long packets) {
      totalDuration += duration;
      totalPackets += packets;
    }
  }
}