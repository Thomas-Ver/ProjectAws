
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(2)
                        .visibilityTimeout(30)
                        .build();

                ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
                List<Message> messages = response.messages();

                if (!messages.isEmpty()) {
                    // Submit each message to the thread pool
                    for (Message message : messages) {
                        executorService.submit(() -> {
                            boolean success = processMessage(message);
                            if (success) {
                                deleteMessage(message);
                            } else {
                                System.out.println("Message not processed, will return to queue: " + message.messageId());
                            }
                        });
                    }
                }

            } catch (Exception e) {
                System.err.println("Error in polling loop: " + e.getMessage());

            }
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

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
        System.out.println("Deleted message: " + message.messageId());
    }
}
