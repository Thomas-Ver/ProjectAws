import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class SummarizeWorker {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static final String OUTPUT_BUCKET = "summarizeworkerlambda280825";

    private static final DateTimeFormatter INPUT_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("dd/MM/yyyy hh:mm:ss a")
            .toFormatter(Locale.ENGLISH);

    public void processFile(String bucketName, String objectKey) {
        try {
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
            String processedCsv = processCsv(s3Object.getObjectContent());

            String outputKey = "daily_summary_" + LocalDate.now() + "_" + objectKey;

            uploadToS3(processedCsv, outputKey);

            deleteFileFromS3(bucketName, objectKey);

            System.out.println("Successfully processed and deleted " + objectKey);

        } catch (Exception e) {
            throw new RuntimeException("Error processing file from bucket: " + bucketName + ", key: " + objectKey, e);
        }
    }

    public String processCsv(InputStream inputStream) throws IOException, CsvException {
        Map<String, AggregatedData> dailyTraffic = new HashMap<>();

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

    private void processRecord(String[] record, Map<String, AggregatedData> dailyTraffic) {
        try {
            String timestampStr = record[6].trim();
            if (timestampStr.isEmpty()) return;

            LocalDateTime timestamp = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);
            String date = timestamp.toLocalDate().toString();

            String sourceIp = record[1].trim();
            String destIp = record[3].trim();

            if (sourceIp.isEmpty() || destIp.isEmpty()) return;

            String key = String.format("%s,%s,%s", date, sourceIp, destIp);

            long flowDuration = parseLongSafely(record[7]);
            long forwardPackets = parseLongSafely(record[8]);

            dailyTraffic.computeIfAbsent(key, k -> new AggregatedData())
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

    private void uploadToS3(String content, String key) throws IOException {
        byte[] contentBytes = content.getBytes();
        try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);
            metadata.setContentType("text/csv");

            PutObjectRequest putRequest = new PutObjectRequest(OUTPUT_BUCKET, key, inputStream, metadata);
            s3Client.putObject(putRequest);
        }
    }

    private void deleteFileFromS3(String bucketName, String objectKey) {
        try {
            s3Client.deleteObject(bucketName, objectKey);
        } catch (AmazonS3Exception e) {
            throw new RuntimeException("Failed to delete object " + objectKey + " from bucket " + bucketName, e);
        }
    }

    private static class AggregatedData {
        long totalDuration = 0;
        long totalPackets = 0;

        void addData(long duration, long packets) {
            totalDuration += duration;
            totalPackets += packets;
        }
    }
}
