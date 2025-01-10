import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.ResponseInputStream;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.util.*;

public class ConsolidateWorker  {

  private final S3Client s3Client;
  private final String outputBucket;

  public ConsolidateWorker(String outputBucket) {
    this.s3Client = S3Client.builder().build();
    this.outputBucket = outputBucket;
  }

  public String run(String sourceBucket, String sourceKey) {
    HashMap<String, AggregatedData> ConsolidateMap = new HashMap<>();
    try {
      try {
        s3Client.headObject(HeadObjectRequest.builder()
            .bucket(sourceBucket)
            .key(sourceKey)
            .build());
      } catch (NoSuchKeyException e) {
        throw new RuntimeException("Object " + sourceKey + " does not exist in bucket " + sourceBucket);
      }

      System.out.println("Attempting to access bucket: " + sourceBucket);
      System.out.println("Attempting to access key: " + sourceKey);

      ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(GetObjectRequest.builder()
          .bucket(sourceBucket)
          .key(sourceKey)
          .build());

      // on lit note hashmap à partir du fichier hashmap.ser dans le bucket
      // consolidate
      ConsolidateMap = ReadConsolidateMapFromS3(outputBucket);
      // on modifie notre hashmap avec les nouvelles données
      processCsv(s3ObjectResponse, ConsolidateMap);

      // on écrit notre hashmap dans le fichier hashmap.ser
      WriteNewhashmapToS3(ConsolidateMap, outputBucket);
      DeleteSummarizeFile(sourceBucket, sourceKey);

      return "Successfully processed " + sourceKey;

    } catch (Exception e) {
      throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
    }
  }

  public void processCsv(InputStream inputStream, HashMap<String, AggregatedData> ConsolidateMap)
      throws IOException, CsvException {

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        CSVReader csvReader = new CSVReader(reader)) {

      List<String[]> records = csvReader.readAll();

      // on va sur chaque ligne en esquivant la premiére
      for (int i = 1; i < records.size(); i++) {
        String[] record = records.get(i);
        // record[0] correspond à la ligne i 1er colone (ici la date)
        processRecord(record, ConsolidateMap);

      }

    }
  }

  private void processRecord(String[] record, HashMap<String, AggregatedData> ConsolidateMap) {

    String sourceIp = record[1];
    String destIp = record[2];

    if (sourceIp.isEmpty() || destIp.isEmpty()) {
      return;
    }

    String key = String.format("%s,%s", sourceIp, destIp);

    String flowDuration = record[3];
    String forwardPackets = record[4];
    String date = record[0];

    List<String> Value = new ArrayList<>();
    Value.add(date);
    Value.add(flowDuration);
    Value.add(forwardPackets);

    // Si la clé n'existe pas on la crée
    ConsolidateMap.putIfAbsent(key, new AggregatedData());
    // On ajoute la valeur à la clé
    ConsolidateMap.get(key).add(Value);

  }

  private HashMap<String, AggregatedData> ReadConsolidateMapFromS3(String bucketName) {
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

  public void WriteNewhashmapToS3(HashMap<String, AggregatedData> map, String bucketName) {
    try {
      // Créer un flux de sortie pour écrire l'objet sérialisé
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

      // Écrire l'objet sérialisé dans le flux de sortie
      objectOutputStream.writeObject(map);
      objectOutputStream.close();

      // Convertir le flux de sortie en tableau de bytes
      byte[] bytes = byteArrayOutputStream.toByteArray();

      // Créer une demande de téléchargement
      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
          .bucket(bucketName)
          .key("hashmap.ser")
          .build();

      // Télécharger l'objet sérialisé sur S3
      s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));

      System.out.println("HashMap sérialisée téléchargée avec succès sur S3.");
    } catch (S3Exception e) {
      System.err.println("Erreur S3 : " + e.awsErrorDetails().errorMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("Erreur d'E/S lors de la sérialisation de l'objet.");
      e.printStackTrace();
    } 
  }
  public void DeleteSummarizeFile(String bucketName, String key) {
    try {
      DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
          .bucket(bucketName)
          .key(key)
          .build();

      s3Client.deleteObject(deleteObjectRequest);

      System.out.println("Fichier supprimé avec succès : " + key);
    } catch (S3Exception e) {
      System.err.println("Erreur S3 : " + e.awsErrorDetails().errorMessage());
      e.printStackTrace();
    } 
  }
}
