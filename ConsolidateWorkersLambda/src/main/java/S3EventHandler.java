
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class S3EventHandler {

    public void handleRequest(SQSEvent event, Context context) {
        Region region = Region.US_EAST_1;
        String queueURL = "https://sqs.us-east-1.amazonaws.com/116404946400/sqs-summarize-worker-021095";
        SqsClient sqsClient = SqsClient.builder().region(region).build();

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueURL)
                .maxNumberOfMessages(1)
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

        if (!messages.isEmpty()) {
            Message msg = messages.get(0);
            try {
                // Parse the JSON
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(msg.body());

                // Extract bucket name and file name from the JSON structure
                String bucketName = jsonNode.get("Records").get(0)
                        .get("s3").get("bucket").get("name").asText();
                String fileName = jsonNode.get("Records").get(0)
                        .get("s3").get("object").get("key").asText();

                S3Client s3 = S3Client.builder().region(region).build();

                // Check file exists
                ListObjectsRequest listObjects = ListObjectsRequest.builder()
                        .bucket(bucketName)
                        .build();
                ListObjectsResponse res = s3.listObjects(listObjects);
                List<S3Object> objects = res.contents();

                if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileName))) {
                    // Retrieve file
                    GetObjectRequest objectRequest = GetObjectRequest.builder()
                            .key(fileName)
                            .bucket(bucketName)
                            .build();

                    ConsolidateWorker worker = new ConsolidateWorker("consolidate-worker-lambda-021095");
                    worker.run(objectRequest);

                    // Delete the message
                    DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                            .queueUrl(queueURL)
                            .receiptHandle(msg.receiptHandle())
                            .build();
                    sqsClient.deleteMessage(deleteMessageRequest);

                    // Delete the file
                    try {
                        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                                .bucket(bucketName)
                                .key(fileName)
                                .build();
                        s3.deleteObject(deleteObjectRequest);
                    } catch (S3Exception e) {
                        System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("File is not available in the Bucket");
                }

            } catch (JsonProcessingException e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("Queue is empty");
        }
    }
}
