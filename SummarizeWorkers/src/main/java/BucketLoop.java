import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class BucketLoop {

    private final S3Client s3Client;
    private final String sourceBucket;
    private final String destinationBucket;
    private final SummarizeWorker worker;
    private volatile boolean isRunning = true;

    public BucketLoop(String sourceBucket, String destinationBucket) {
        this.s3Client = S3Client.builder().build();
        this.sourceBucket = sourceBucket;
        this.destinationBucket = destinationBucket;
        this.worker = new SummarizeWorker();
    }

    public void start() {
        System.out.println("Starting S3 File Processor...");

        while (isRunning) {
            try {
                
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                        .bucket(sourceBucket)
                        .maxKeys(1) 
                        .build();

                ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

                if (!listResponse.contents().isEmpty()) {
                    
                    S3Object fileToProcess = listResponse.contents().get(0);
                    String fileKey = fileToProcess.key();

                    System.out.println("Processing file: " + fileKey);

                    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                            .bucket(sourceBucket)
                            .key(fileKey)
                            .build();

                    String processedCsv = null;

                    
                    try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(getObjectRequest)) {
                        
                        processedCsv = worker.processCsv(s3ObjectResponse);
                    } catch (Exception e) {
                        System.err.println("Error processing file: " + e.getMessage());
                        e.printStackTrace();
                    }

                    String outputKey = "daily_summary_" + java.time.LocalDate.now() + "_" + fileKey;
                    
                    sendFileToAnotherBucket(processedCsv, outputKey);

                    deleteFileFromSourceBucket(fileKey);

                } else {
                    System.out.println("Source bucket is empty. Waiting...");
                    Thread.sleep(5000); 
                }

            } catch (Exception e) {
                System.err.println("Error in file processing loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void sendFileToAnotherBucket(String content, String key) throws IOException {
        byte[] contentBytes = content.getBytes();
        try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(destinationBucket)
                    .key(key)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromBytes(contentBytes));
        }
    }

    private void deleteFileFromSourceBucket(String fileKey) {
        try {
            System.out.println("Deleting file from source bucket: " + fileKey);

            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(fileKey)
                    .build();

            s3Client.deleteObject(deleteRequest);

            System.out.println("File successfully deleted from source bucket: " + fileKey);

        } catch (Exception e) {
            System.err.println("Error deleting file from source bucket: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void stop() {
        isRunning = false;
        System.out.println("Stopping S3 File Processor...");
    }

    public static void main(String[] args) {
        String sourceBucket = "s3-raw-data-ec2-021095";
        String destinationBucket = "s3-summarized-data-ec2-021095";

        BucketLoop app = new BucketLoop(sourceBucket, destinationBucket);
        app.start();
    }
}
