import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;



public class S3EventHandler implements RequestHandler<S3Event, String> {
  private final String outputBucket = "consolidate-worker-ec2-021095";

  @Override
  public String handleRequest(S3Event s3event, Context context) {
    
    String sourceBucket = s3event.getRecords().get(0).getS3().getBucket().getName();
    String sourceKey = s3event.getRecords().get(0).getS3().getObject().getKey();
      
    ConsolidateWorker worker = new ConsolidateWorker(outputBucket);
    worker.run(sourceBucket, sourceKey);
    
    return "Success";
  }


  }

