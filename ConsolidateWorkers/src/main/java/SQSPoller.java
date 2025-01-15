
import java.util.List;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SQSPoller {

    private final S3Client s3Client;
    private final SqsClient sqsClient;
    private final String sourceBucket;
    private final String queueUrl;
    private final ConsolidateWorker worker;
    private volatile boolean isRunning = true;

    public SQSPoller(String sourceBucket, String outputBucket, String queueUrl) {
        this.s3Client = S3Client.builder().build();
        this.sqsClient = SqsClient.builder().build();
        this.sourceBucket = sourceBucket;
        this.queueUrl = queueUrl;
        this.worker = new ConsolidateWorker(outputBucket);
    }

    public void start() {
        System.out.println("Starting SQS Message Processor...");

        while (isRunning) {
            try {
                // Request to receive messages from SQS
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build();

                // Receive messages from the queue
                List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

                if (!messages.isEmpty()) {
                    for (Message message : messages) {
                        processMessage(message);
                        deleteMessage(message);
                    }
                } else {
                    System.out.println("No messages available. Waiting...");
                }
            } catch (Exception e) {
                System.err.println("Error in message processing loop: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void processMessage(Message message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            S3EventNotification s3Event = mapper.readValue(message.body(), S3EventNotification.class);

            boolean processedSuccessfully = true;
            for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
                String bucketName = record.getS3().getBucket().getName();
                String objectKey = record.getS3().getObject().getKey();

                System.out.printf("Processing file: %s from bucket: %s%n", objectKey, bucketName);

                try {
                    processS3Object(bucketName, objectKey);
                } catch (Exception e) {
                    processedSuccessfully = false;
                    System.err.println("Error processing object: " + objectKey + " - " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // Only delete the message if processing was successful
            if (processedSuccessfully) {
                deleteMessage(message);
            }
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processS3Object(String sourceBucket, String fileKey) {
        try {
            // Process the S3 object
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(fileKey)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(getObjectRequest)) {
                worker.run(s3ObjectResponse, fileKey);
            }

            // Delete the processed file from S3
            deleteFileFromSourceBucket(fileKey);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void deleteMessage(Message message) {
        try {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteRequest);
        } catch (Exception e) {
            System.err.println("Error deleting message from SQS: " + e.getMessage());
            e.printStackTrace();
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
        System.out.println("Stopping SQS Message Processor...");
    }

    public static void main(String[] args) {
        String sourceBucket = "s3-summarized-data-ec2-021095";
        String destinationBucket = "s3-consolidated-data-ec2-021095";
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/979238852085/sqs-to-consolidate-worker-ec2";  

        SQSPoller app = new SQSPoller(sourceBucket, destinationBucket, queueUrl);
        app.start();
    }
}

