import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SQSLambdaHandler {

    public void handleRequest(SQSEvent sqsEvent, Context context) {
        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            processMessage(message, context);
        }
    }

    private void processMessage(SQSEvent.SQSMessage message, Context context) {
        try {
            String body = message.getBody();
            context.getLogger().log("Raw message body: " + body);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(body);

            JsonNode recordsNode = root.path("Records");
            if (recordsNode.isEmpty() || !recordsNode.isArray()) {
                context.getLogger().log("No Records array found in message. Message content: " + body);
                return;
            }

            JsonNode firstRecord = recordsNode.get(0);
            String bucketName = firstRecord.path("s3").path("bucket").path("name").asText();
            String objectKey = firstRecord.path("s3").path("object").path("key").asText();

            if (bucketName.isEmpty() || objectKey.isEmpty()) {
                context.getLogger().log("Empty bucket name or object key");
                return;
            }

            context.getLogger().log("Processing S3 event - Bucket: " + bucketName + ", Key: " + objectKey);

            objectKey = URLDecoder.decode(objectKey, StandardCharsets.UTF_8.name());

            SummarizeWorker summarizeWorker = new SummarizeWorker();
            summarizeWorker.processFile(bucketName, objectKey);

        } catch (Exception e) {
            context.getLogger().log("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void sendMessage(String fileName) {
        Region region = Region.US_EAST_1;

        String queueURL = "https://sqs.us-east-1.amazonaws.com/116404946400/sqs-summarize-worker-021095";
        String bucketName = "summarize-worker-lambda-021095";

        SqsClient sqsClient = SqsClient.builder().region(region).build();

        SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(queueURL)
                .messageBody(bucketName + ";" + fileName).build();

        SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);

        System.out.println(
                sqsResponse.messageId() + " Message sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
    }
}