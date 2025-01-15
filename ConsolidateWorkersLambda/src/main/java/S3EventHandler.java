
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3EventHandler implements RequestHandler<SQSEvent, String> {

    private final S3Client s3Client;
    private final String outputBucket = "s3-consolidated-data-021095";

    public S3EventHandler() {
        this.s3Client = S3Client.builder().build();
    }

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                // Parse the JSON message
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(message.getBody());

                // Extract bucket name and file name from the S3 event
                String bucketName = jsonNode.get("Records").get(0)
                        .get("s3").get("bucket").get("name").asText();
                String fileName = jsonNode.get("Records").get(0)
                        .get("s3").get("object").get("key").asText();

                ConsolidateWorker worker = new ConsolidateWorker(outputBucket);
                worker.run(bucketName, fileName);

                // Delete the processed file
                deleteS3Object(bucketName, fileName);

            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                throw new RuntimeException("Error processing SQS message", e);
            }
        }
        return "Processing completed";
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
}
