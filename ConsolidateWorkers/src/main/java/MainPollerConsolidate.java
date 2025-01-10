public class MainPollerConsolidate {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: java -jar ConsolidateSqsPoller.jar <queueUrl> <outputBucket>");
      System.exit(1);
    }
    
    String queueUrl = args[0];
    String outputBucket = args[1];

    
    ConsolidateWorker worker = new ConsolidateWorker(outputBucket);
    
    SqsPollerApp poller = new SqsPollerApp(queueUrl, worker);
    
    poller.startPolling();
  }
}
