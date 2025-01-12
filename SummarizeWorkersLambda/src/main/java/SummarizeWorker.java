
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;

public class SummarizeWorker implements RequestHandler<S3Event, String> {

    private final S3Handler s3Handler;
    private final CsvProcessor csvProcessor;

    public SummarizeWorker() {
        this.csvProcessor = new CsvProcessor();
        this.s3Handler = new S3Handler();
    }

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
                String bucketName = record.getS3().getBucket().getName();
                String objectKey = URLDecoder.decode(
                        record.getS3().getObject().getKey(),
                        StandardCharsets.UTF_8.toString()
                );

                processFile(bucketName, objectKey);
            }
            return "Successfully processed all files";

        } catch (Exception e) {
            String errorMessage = "Error processing S3 event: " + e.getMessage();
            System.err.println(errorMessage);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public void processFile(String bucketName, String objectKey) {
        try {
            S3Object s3Object = s3Handler.getObject(bucketName, objectKey);
            String processedCsv = csvProcessor.processCsv(s3Object.getObjectContent());

            String outputKey = csvProcessor.generateOutputKey(objectKey);

            s3Handler.uploadToS3(processedCsv, outputKey);
            s3Handler.deleteFileFromS3(bucketName, objectKey);

            System.out.println("Successfully processed and deleted " + objectKey);

        } catch (Exception e) {
            throw new RuntimeException("Error processing file from bucket: " + bucketName + ", key: " + objectKey, e);
        }
    }
}
