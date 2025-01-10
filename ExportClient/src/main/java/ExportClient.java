import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;
import java.io.*;
import java.util.*;


public class ExportClient {

    public static String bucketName = "consolidateworkerlambda280825";
    public static String keyName = "hashmap.ser";
    public static Scanner scanner = new Scanner(System.in);
    

    public static void main(String[] args) {

      boolean bool = true;
      S3Client s3 = S3Client.builder().build();
      HashMap<String, AggregatedData> Consolidatemap = new ExportClient().ReadConsolidateMapFromS3(bucketName, s3);
      while (bool){

        System.out.println("source IP: ");
        String sourceIP = scanner.nextLine();
        System.out.println("destination IP: ");
        String destinationIP = scanner.nextLine();
        if (Consolidatemap.containsKey(sourceIP +","+ destinationIP)){
          System.out.println("La clé existe dans la HashMap");
          CreatCsvFile(Consolidatemap.get(sourceIP +","+ destinationIP),sourceIP,destinationIP);
          
          
        } else {
          System.out.println("La clé n'existe pas dans la HashMap");
          
        }
      }

}


  private HashMap<String, AggregatedData> ReadConsolidateMapFromS3(String bucketName, S3Client s3Client) {
    HashMap<String, AggregatedData> map = null;

    try {
      // Vérifiez si le fichier existe sur S3
      if (!fileExistsOnS3(s3Client, bucketName, "hashmap.ser")) {
        System.out.println("Le fichier hashmap.set n'existe pas. Initialisation d'une HashMap vide.");
        return new HashMap<>(); // Retourne une HashMap vide
      }
      // Créer une requête pour obtenir l'objet S3
      GetObjectRequest getObjectRequest = GetObjectRequest.builder()
          .bucket(bucketName)
          .key("hashmap.ser")
          .build();

      // Récupérer l'objet S3 en tant que flux d'entrée
      ResponseInputStream<?> s3ObjectStream = s3Client.getObject(getObjectRequest);

      // Utiliser try-with-resources pour s'assurer que les flux sont fermés
      try (InputStream inputStream = s3ObjectStream;
          ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {

        // Désérialiser l'objet
        Object obj = objectInputStream.readObject();
        if (obj instanceof HashMap) {
          // Suppression des avertissements de type
          @SuppressWarnings("unchecked")
          HashMap<String, AggregatedData> tempMap = (HashMap<String, AggregatedData>) obj;
          map = tempMap;
        } else {
          System.err.println("L'objet désérialisé n'est pas une HashMap.");
        }
      }

      System.out.println("HashMap chargée depuis S3 avec succès.");
    } catch (S3Exception e) {
      System.err.println("Erreur S3 : " + e.awsErrorDetails().errorMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("Erreur d'E/S lors de la lecture de l'objet S3.");
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.err.println("Classe non trouvée lors de la désérialisation.");
      e.printStackTrace();
    } 

    return map;
  }


  public static boolean fileExistsOnS3(S3Client s3, String bucketName, String key) {
    try {
      HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
          .bucket(bucketName)
          .key(key)
          .build();

      HeadObjectResponse response = s3.headObject(headObjectRequest);
      return response != null;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return false; // Le fichier n'existe pas
      } else {
        System.err
            .println(
                "Erreur lors de la vérification de l'existence du fichier : " + e.awsErrorDetails().errorMessage());
        throw e;
      }
    }
  }
  public static void CreatCsvFile(AggregatedData data, String sourceIp, String destinationIp) {
    try {
      FileWriter writer = new FileWriter("data_"+sourceIp+":"+destinationIp+".csv");
      writer.append("Source IP");
      writer.append(",");
      writer.append("Destination IP");
      writer.append(",");
      writer.append("Mean TotalFlowDuration");
      writer.append(",");
      writer.append("Standard Deviation TotalFlowDuration");
      writer.append(",");
      writer.append("Mean TotalPacketsForward");
      writer.append(",");
      writer.append("Standard Deviation TotalPacketsForward");
      writer.append("\n");
      writer.append(sourceIp);
      writer.append(",");
      writer.append(destinationIp);
      writer.append(",");
      writer.append(String.valueOf(data.MeanTfdPerday));
      writer.append(",");
      writer.append(String.valueOf(Math.sqrt(data.VarianceTfdPerday)));
      writer.append(",");
      writer.append(String.valueOf(data.MeanTfpPerday));
      writer.append(",");
      writer.append(String.valueOf(Math.sqrt(data.VarianceTfpPerday)));
      writer.append("\n");
      writer.append("\n");
      writer.append("Date");
      writer.append(",");
      writer.append("TotalFlowDuration");
      writer.append(",");
      writer.append("TotalPacketsForward");
      writer.append("\n");
      for (List<String> L : data.data){
        writer.append(L.get(0));
        writer.append(",");
        writer.append(L.get(1));
        writer.append(",");
        writer.append(L.get(2));
        writer.append("\n");
      }


      writer.flush();
      writer.close();
      System.out.println("Fichier CSV créé avec succès.");
    } catch (IOException e) {
      System.err.println("Erreur lors de la création du fichier CSV.");
      e.printStackTrace();
    }
  }
}