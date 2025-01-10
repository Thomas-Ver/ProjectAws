import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class SqsPollerApp {
  
  private final SqsClient sqsClient;
  private final String queueUrl;
  private final SummarizeWorker summarizeWorker;
  
  public SqsPollerApp(String queueUrl, SummarizeWorker summarizeWorker) {
    this.sqsClient = SqsClient.builder().build();
    this.queueUrl = queueUrl;
    this.summarizeWorker = summarizeWorker;
  }
  
  public void startPolling() {
    System.out.println("Starting SQS polling on queue: " + queueUrl);
    
    while (true) {
      try {
        
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
          .queueUrl(queueUrl)
          .maxNumberOfMessages(5)           
          .waitTimeSeconds(10)              
          .visibilityTimeout(30)            
          .build();
        
        ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
        List<Message> messages = response.messages();
        
        if (messages.isEmpty()) {
          
          continue;
        }
        
        
        for (Message message : messages) {
          boolean success = processMessage(message);
          if (success) {
            
            deleteMessage(message);
          } else {
            
            System.out.println("Message not processed, will return to queue: " + message.messageId());
          }
        }
        
      } catch (Exception e) {
        System.err.println("Error in polling loop: " + e.getMessage());
        
      }
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
