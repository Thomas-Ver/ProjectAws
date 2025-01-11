
import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;   

public class SqsPollerApp {

    private final SqsClient sqsClient;
    private final String queueUrl;
    private final SummarizeWorker summarizeWorker;
    private final ExecutorService executorService;
    private final int numThreads = 5;
    private volatile boolean isRunning = true;

    public SqsPollerApp(String queueUrl, SummarizeWorker summarizeWorker) {
        this.sqsClient = SqsClient.builder().build();
        this.queueUrl = queueUrl;
        this.summarizeWorker = summarizeWorker;
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    public void startPolling() {
        System.out.println("Starting SQS polling on queue: " + queueUrl);

        while (isRunning) {
            try {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10) // Fetch up to 10 messages per poll
                        .waitTimeSeconds(10) // Long polling for efficiency
                        .visibilityTimeout(120) // Adjust timeout to allow full processing
                        .build();

                ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
                List<Message> messages = response.messages();

                if (!messages.isEmpty()) {
                    // Submit the entire batch for processing
                    executorService.submit(() -> processBatch(messages));
                }

            } catch (Exception e) {
                System.err.println("Error in polling loop: " + e.getMessage());
            }
        }
    }

    private void processBatch(List<Message> messages) {
        try {
            System.out.println("Processing batch of " + messages.size() + " messages");

            // Step 1: Download all files asynchronously
            List<CompletableFuture<Void>> downloadTasks = new ArrayList<>();
            for (Message message : messages) {
                downloadTasks.add(CompletableFuture.runAsync(() -> {
                    try {
                        String fileKey = extractFileKeyFromMessage(message);
                        String bucketName = extractBucketNameFromMessage(message);
                        downloadFileFromS3(bucketName, fileKey); // Custom method to download the file
                    } catch (Exception e) {
                        System.err.println("Error downloading file: " + e.getMessage());
                    }
                }));
            }

            // Wait for all downloads to complete
            CompletableFuture.allOf(downloadTasks.toArray(new CompletableFuture[0])).join();
            System.out.println("All files downloaded for the batch");

            // Step 2: Process each message
            for (Message message : messages) {
                boolean success = processMessage(message);
                if (success) {
                    deleteMessage(message); // Only delete the message if successfully processed
                } else {
                    System.err.println("Failed to process message: " + message.messageId());
                }
            }

        } catch (Exception e) {
            System.err.println("Error in batch processing: " + e.getMessage());
        }
    }

    private String extractFileKeyFromMessage(Message message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(message.body());

            // Handle wrapped SNS message
            if (root.has("Message")) {
                root = objectMapper.readTree(root.get("Message").asText());
            }

            JsonNode firstRecord = root.path("Records").get(0);
            return URLDecoder.decode(firstRecord.path("s3").path("object").path("key").asText(), StandardCharsets.UTF_8.name());

        } catch (Exception e) {
            throw new RuntimeException("Error extracting file key: " + e.getMessage(), e);
        }
    }

    private String extractBucketNameFromMessage(Message message) {
        try {
            // Parse the message body as JSON
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(message.body());

            // Handle the case where the message is wrapped in SNS
            if (root.has("Message")) {
                root = objectMapper.readTree(root.get("Message").asText());
            }

            // Extract the "bucket" name from the first record in the "Records" array
            JsonNode recordsNode = root.path("Records");
            if (recordsNode.isEmpty() || !recordsNode.isArray()) {
                throw new RuntimeException("No 'Records' array found in message.");
            }

            JsonNode firstRecord = recordsNode.get(0);
            String bucketName = firstRecord.path("s3").path("bucket").path("name").asText();

            if (bucketName.isEmpty()) {
                throw new RuntimeException("Bucket name is empty in the SQS message.");
            }

            return bucketName;

        } catch (Exception e) {
            throw new RuntimeException("Error extracting bucket name: " + e.getMessage(), e);
        }
    }


    private void downloadFileFromS3(String bucketName, String fileKey) {
        // Initialize the S3 Client
        try (S3Client s3Client = S3Client.builder().build()) {
            System.out.println("Downloading file from S3: Bucket=" + bucketName + ", Key=" + fileKey);

            // Define the local file path where the file will be downloaded
            String localFilePath = "downloaded_files/" + fileKey;
            File localFile = new File(localFilePath);

            // Ensure the parent directory exists
            localFile.getParentFile().mkdirs();

            // Create GetObjectRequest to specify bucket and object details
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileKey)
                    .build();

            // Download the file
            s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(Paths.get(localFilePath)));

            System.out.println("File downloaded successfully to: " + localFilePath);
        } catch (Exception e) {
            System.err.println("Error downloading file from S3: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to download file from S3", e);
        }
    }

    private boolean processMessage(Message message) {
        try {
            String body = message.body();
            System.out.println("Raw message body: " + body);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(body);

            // Check if this is a test event
            if (root.has("Event") && root.get("Event").asText().equals("s3:TestEvent")) {
                System.out.println("Received test event from S3, skipping processing");
                return true; // Mark as processed since we don't need to retry test events
            }

            // If it's wrapped in SNS, parse the inner message
            if (root.has("Message")) {
                root = objectMapper.readTree(root.get("Message").asText());
            }

            JsonNode recordsNode = root.path("Records");
            if (recordsNode.isEmpty() || !recordsNode.isArray()) {
                System.out.println("No Records array found in message. Message content: " + body);
                return false;
            }

            JsonNode firstRecord = recordsNode.get(0);
            String bucketName = firstRecord.path("s3").path("bucket").path("name").asText();
            String objectKey = firstRecord.path("s3").path("object").path("key").asText();

            if (bucketName.isEmpty() || objectKey.isEmpty()) {
                System.out.println("Empty bucket name or object key");
                return false;
            }

            System.out.println("Processing S3 event - Bucket: " + bucketName + ", Key: " + objectKey);

            objectKey = URLDecoder.decode(objectKey, StandardCharsets.UTF_8.name());
            summarizeWorker.run(bucketName, objectKey);

            return true;

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public void shutdown() {
        isRunning = false;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
        System.out.println("Deleted message: " + message.messageId());
    }
}
