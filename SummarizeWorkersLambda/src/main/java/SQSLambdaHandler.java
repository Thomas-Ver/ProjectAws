import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
}