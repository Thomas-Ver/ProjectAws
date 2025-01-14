
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.exceptions.CsvException;

import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;

public class CsvProcessor {

    private static final DateTimeFormatter INPUT_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("dd/MM/yyyy hh:mm:ss a")
            .toFormatter(Locale.ENGLISH);

    public void processFile(String bucketName, String objectKey) {
        S3Handler s3Handler = new S3Handler();
        try {
            S3Object s3Object = s3Handler.getObject(bucketName, objectKey);
            String processedCsv = processCsv(s3Object.getObjectContent());
            SQSLambdaHandler sqsLambdaHandler = new SQSLambdaHandler();

            String outputKey = "daily_summary_" + LocalDate.now() + "_" + objectKey;

            s3Handler.uploadToS3(processedCsv, outputKey);
            s3Handler.deleteFileFromS3(bucketName, objectKey);

            System.out.println("Successfully processed and deleted " + objectKey);

        } catch (Exception e) {
            throw new RuntimeException("Error processing file from bucket: " + bucketName + ", key: " + objectKey, e);
        }
    }

    public String processCsv(InputStream inputStream) throws IOException, CsvException {
        Map<String, AggregatedData> dailyTraffic = Collections.synchronizedMap(new HashMap<>());

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream), 32768); CsvReader csvReader = CsvReader.builder().ofCsvRecord(reader)) {

            try (Stream<CsvRecord> stream = csvReader.stream()) {
                stream.skip(1)
                        .parallel()
                        .forEach(row -> processRecord(row, dailyTraffic));
            }

            return convertToOutput(dailyTraffic);
        }
    }

    private void processRecord(CsvRecord row, Map<String, AggregatedData> dailyTraffic) {
        try {
            if (row.getFieldCount() < 9) {
                return;
            }

            String timestampStr = row.getField(6).trim();
            if (timestampStr.isEmpty()) {
                return;
            }

            LocalDateTime timestamp = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);
            String date = timestamp.toLocalDate().toString();

            String sourceIp = row.getField(1).trim();
            String destIp = row.getField(3).trim();

            if (sourceIp.isEmpty() || destIp.isEmpty()) {
                return;
            }

            String key = String.format("%s,%s,%s", date, sourceIp, destIp);

            long flowDuration = parseLongSafely(row.getField(7));
            long forwardPackets = parseLongSafely(row.getField(8));

            dailyTraffic.computeIfAbsent(key, k -> new AggregatedData())
                    .addData(flowDuration, forwardPackets);

        } catch (Exception e) {
            System.err.println("Error processing record: " + row.getFields());
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

    public String generateOutputKey(String originalObjectKey) {
        return "daily_summary_" + LocalDate.now() + "_" + originalObjectKey;
    }

    private String convertToOutput(Map<String, AggregatedData> dailyTraffic) {
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

    private static class AggregatedData {

        long totalDuration = 0;
        long totalPackets = 0;

        void addData(long duration, long packets) {
            totalDuration += duration;
            totalPackets += packets;
        }
    }
}
