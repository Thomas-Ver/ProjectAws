import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.*;
import java.util.*;


public class ExportClient {

    public static String bucketName = "consolidateworker280825";
    public static String keyName = "hashmap.ser";
    public static String filePath = "/mnt/c/Users/thoma/OneDrive/Bureau/3A_ICM/majeur_info/Cloud_and_edge/FinalProject/ExportClient";
    

    public static void main(String[] args) {


      S3Client s3 = S3Client.builder().build();
      HashMap<String, AggregatedData> Consolidatemap = new ExportClient().ReadConsolidateMapFromS3(bucketName, s3);
      System.out.println("HashMap chargée depuis S3 : " + Consolidatemap);
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
}