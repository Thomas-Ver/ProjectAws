
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Handler {

    private final AmazonS3 s3Client;
    private static final String OUTPUT_BUCKET = "summarize-worker-lambda-021095";

    public S3Handler() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
    }

    public S3Object getObject(String bucketName, String objectKey) {
        return s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
    }

    public void uploadToS3(String content, String key) throws IOException {
        byte[] contentBytes = content.getBytes();
        try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentBytes.length);
            metadata.setContentType("text/csv");

            PutObjectRequest putRequest = new PutObjectRequest(OUTPUT_BUCKET, key, inputStream, metadata);
            s3Client.putObject(putRequest);
        }
    }

    public void deleteFileFromS3(String bucketName, String objectKey) {
        try {
            s3Client.deleteObject(bucketName, objectKey);
        } catch (AmazonS3Exception e) {
            throw new RuntimeException("Failed to delete object " + objectKey + " from bucket " + bucketName, e);
        }
    }
}
