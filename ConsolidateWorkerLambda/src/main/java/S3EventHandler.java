import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3EventHandler implements RequestHandler<S3Event, String> {

    private final S3Client s3Client;
    private final String outputBucket = "s3-consolidated-data-lambda-021095";

    public S3EventHandler() {
        this.s3Client = S3Client.builder().build();
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        try {
            for (S3EventNotificationRecord record : event.getRecords()) {
                String bucketName = record.getS3().getBucket().getName();
                String fileName = record.getS3().getObject().getKey();

                if (fileName.endsWith(".csv")) {
                    context.getLogger().log("Processing file: " + fileName);

                    ConsolidateWorker worker = new ConsolidateWorker(outputBucket);
                    worker.run(bucketName, fileName);

                    deleteS3Object(bucketName, fileName);

                    context.getLogger().log("Successfully processed file: " + fileName);
                }
            }
            return "Processing completed successfully";
        } catch (Exception e) {
            context.getLogger().log("Error processing event: " + e.getMessage());
            throw new RuntimeException("Error processing S3 event", e);
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
}
