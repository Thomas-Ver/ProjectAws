public class MainPoller {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: java -jar SqsPoller.jar <queueUrl> <outputBucket>");
      System.exit(1);
    }
    
    String queueUrl = args[0];
    String outputBucket = args[1];
    
    SummarizeWorker worker = new SummarizeWorker(outputBucket);
    
    SqsPollerApp poller = new SqsPollerApp(queueUrl, worker);
    
    poller.startPolling();
  }
}
