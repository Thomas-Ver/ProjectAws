
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class ConsolidateWorker {

    private final S3Client s3Client;
    private final String outputBucket;
    private final String inputBucket;// Optional: to process files from specific folder

    public ConsolidateWorker(String inputBucket, String outputBucket) {
        this.s3Client = S3Client.builder().build();
        this.inputBucket = inputBucket;
        this.outputBucket = outputBucket;
    }

    public ConsolidateWorker(String outputBucket) {
        this.s3Client = S3Client.builder().build();
        this.outputBucket = outputBucket;
        this.inputBucket = null; // Not needed for single file processing
    }

    public void run(String sourceBucket, String sourceKey) {
        try {
            String outputKey = String.format("batches/%s_%s.ser",
                    UUID.randomUUID().toString(),
                    sourceKey.replace('/', '_'));

            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();

            HashMap<String, List<List<String>>> batchMap = new HashMap<>();

            try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(request)) {
                processCsv(s3ObjectResponse, batchMap);
            }

            WriteNewhashmapToS3(batchMap, outputBucket, outputKey);

            System.out.println("Successfully processed " + sourceKey);
        } catch (Exception e) {
            throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
        }
    }

    public void processAllFiles() {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(inputBucket)
                    .build();

            ListObjectsV2Response listResponse;
            do {
                listResponse = s3Client.listObjectsV2(listRequest);
                for (S3Object s3Object : listResponse.contents()) {
                    if (s3Object.key().endsWith(".csv")) { // Process only CSV files
                        processFile(s3Object.key());
                    }
                }
                listRequest = ListObjectsV2Request.builder()
                        .bucket(inputBucket)
                        .continuationToken(listResponse.nextContinuationToken())
                        .build();
            } while (listResponse.isTruncated());

        } catch (S3Exception e) {
            throw new RuntimeException("Error listing S3 objects: " + e.getMessage(), e);
        }
    }

    private void processFile(String sourceKey) {
        try {
            String outputKey = String.format("batches/%s_%s.ser",
                    UUID.randomUUID().toString(),
                    sourceKey.replace('/', '_'));

            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(inputBucket)
                    .key(sourceKey)
                    .build();

            HashMap<String, List<List<String>>> batchMap = new HashMap<>();

            try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(request)) {
                processCsv(s3ObjectResponse, batchMap);
            }

            WriteNewhashmapToS3(batchMap, outputBucket, outputKey);

            // Delete processed file
            deleteS3Object(inputBucket, sourceKey);

            System.out.println("Successfully processed " + sourceKey);
        } catch (Exception e) {
            throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
        }
    }

    public void processCsv(InputStream inputStream, HashMap<String, List<List<String>>> batchMap)
            throws IOException, CsvException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)); CSVReader csvReader = new CSVReader(reader)) {

            List<String[]> records = csvReader.readAll();
            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);
                processRecord(record, batchMap);
            }
        }
    }

    private void processRecord(String[] record, HashMap<String, List<List<String>>> ConsolidateMap) {
        String sourceIp = record[1];
        String destIp = record[2];

        if (sourceIp.isEmpty() || destIp.isEmpty()) {
            return;
        }

        String key = String.format("%s,%s", sourceIp, destIp);
        String flowDuration = record[3];
        String forwardPackets = record[4];
        String date = record[0];

        // Get or create the list for this key
        List<List<String>> existingValues = ConsolidateMap.computeIfAbsent(key, k -> new ArrayList<>());

        // Create the new value list
        List<String> newValue = new ArrayList<>();
        newValue.add(date);
        newValue.add(flowDuration);
        newValue.add(forwardPackets);

        // Check if this exact combination already exists
        boolean valueExists = existingValues.stream().anyMatch(existing
                -> existing.get(0).equals(date)
                && existing.get(1).equals(flowDuration)
                && existing.get(2).equals(forwardPackets)
        );

        // Only add if this exact combination doesn't exist
        if (!valueExists) {
            existingValues.add(newValue);
        }
    }

    private void deleteS3Object(String bucketName, String key) {
        try {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            s3Client.deleteObject(deleteRequest);
        } catch (S3Exception e) {
            throw new RuntimeException("Error deleting S3 object: " + e.getMessage(), e);
        }
    }

    // public static boolean fileExistsOnS3(S3Client s3, String bucketName, String key) {
    //     try {
    //         HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
    //                 .bucket(bucketName)
    //                 .key(key)
    //                 .build();
    //         HeadObjectResponse response = s3.headObject(headObjectRequest);
    //         return response != null;
    //     } catch (S3Exception e) {
    //         if (e.statusCode() == 404) {
    //             return false; // Le fichier n'existe pas
    //         } else {
    //             System.err
    //                     .println(
    //                             "Erreur lors de la vérification de l'existence du fichier : " + e.awsErrorDetails().errorMessage());
    //             throw e;
    //         }
    //     }
    // }
    public void WriteNewhashmapToS3(HashMap<String, List<List<String>>> map, String bucketName, String outputKey) {
        try {
            // Create output stream for serialized object
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            // Write serialized object to output stream
            objectOutputStream.writeObject(map);
            objectOutputStream.close();

            // Convert output stream to byte array
            byte[] bytes = byteArrayOutputStream.toByteArray();

            // Create upload request
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(outputKey) // Use the provided outputKey instead of hardcoded "hashmap.ser"
                    .build();

            // Upload serialized object to S3
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));

            System.out.println("Successfully uploaded batch file to S3: " + outputKey);
        } catch (S3Exception e) {
            System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Failed to write to S3", e);
        } catch (IOException e) {
            System.err.println("IO Error during object serialization.");
            throw new RuntimeException("Failed to serialize map", e);
        }
    }
}
