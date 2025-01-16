
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ConsolidateWorker {

    private final S3Client s3Client;
    private final String outputBucket;

    public ConsolidateWorker(String outputBucket) {
        this.s3Client = S3Client.builder().build();
        this.outputBucket = outputBucket;
    }

    public String run(String sourceBucket, String sourceKey) {
        try {
            // Generate a unique output key for this batch
            String outputKey = String.format("batches/%s_%s.ser",
                    UUID.randomUUID().toString(),
                    sourceKey.replace('/', '_'));

            // Process the incoming CSV file
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();

            HashMap<String, List<List<String>>> batchMap = new HashMap<>();

            try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(request)) {
                processCsv(s3ObjectResponse, batchMap);
            }

            // Write this batch's data to its own file
            WriteNewhashmapToS3(batchMap, outputBucket, outputKey);

            return "Successfully processed " + sourceKey;
        } catch (Exception e) {
            throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
        }
    }

    public void processCsv(InputStream inputStream, HashMap<String, List<List<String>>> batchMap)
            throws IOException, CsvException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)); CSVReader csvReader = new CSVReader(reader)) {

            List<String[]> records = csvReader.readAll();
            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);
                processRecord(record, batchMap);
            }
        }
    }

    private void processRecord(String[] record, HashMap<String, List<List<String>>> ConsolidateMap) {
        String sourceIp = record[1];
        String destIp = record[2];

        if (sourceIp.isEmpty() || destIp.isEmpty()) {
            return;
        }

        String key = String.format("%s,%s", sourceIp, destIp);
        String flowDuration = record[3];
        String forwardPackets = record[4];
        String date = record[0];

        // Get or create the list for this key
        List<List<String>> existingValues = ConsolidateMap.computeIfAbsent(key, k -> new ArrayList<>());

        // Create the new value list
        List<String> newValue = new ArrayList<>();
        newValue.add(date);
        newValue.add(flowDuration);
        newValue.add(forwardPackets);

        // Check if this exact combination already exists
        boolean valueExists = existingValues.stream().anyMatch(existing
                -> existing.get(0).equals(date)
                && existing.get(1).equals(flowDuration)
                && existing.get(2).equals(forwardPackets)
        );

        // Only add if this exact combination doesn't exist
        if (!valueExists) {
            existingValues.add(newValue);
        }
    }

    // private HashMap<String, List<List<String>>> ReadConsolidateMapFromS3(String bucketName) {
    //     HashMap<String, List<List<String>>> map = null;

    //     try {
    //         // Vérifiez si le fichier existe sur S3
    //         if (!fileExistsOnS3(s3Client, bucketName, "hashmap.ser")) {
    //             System.out.println("Le fichier hashmap.set n'existe pas. Initialisation d'une HashMap vide.");
    //             return new HashMap<>(); // Retourne une HashMap vide
    //         }
    //         // Créer une requête pour obtenir l'objet S3
    //         GetObjectRequest getObjectRequest = GetObjectRequest.builder()
    //                 .bucket(bucketName)
    //                 .key("hashmap.ser")
    //                 .build();

    //         // Récupérer l'objet S3 en tant que flux d'entrée
    //         ResponseInputStream<?> s3ObjectStream = s3Client.getObject(getObjectRequest);

    //         // Utiliser try-with-resources pour s'assurer que les flux sont fermés
    //         try (InputStream inputStream = s3ObjectStream; ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {

    //             // Désérialiser l'objet
    //             Object obj = objectInputStream.readObject();
    //             if (obj instanceof HashMap) {
    //                 // Suppression des avertissements de type
    //                 @SuppressWarnings("unchecked")
    //                 HashMap<String, List<List<String>>> tempMap = (HashMap<String, List<List<String>>>) obj;
    //                 map = tempMap;
    //             } else {
    //                 System.err.println("L'objet désérialisé n'est pas une HashMap.");
    //             }
    //         }

    //         System.out.println("HashMap chargée depuis S3 avec succès.");
    //     } catch (S3Exception e) {
    //         System.err.println("Erreur S3 : " + e.awsErrorDetails().errorMessage());
    //         e.printStackTrace();
    //     } catch (IOException e) {
    //         System.err.println("Erreur d'E/S lors de la lecture de l'objet S3.");
    //         e.printStackTrace();
    //     } catch (ClassNotFoundException e) {
    //         System.err.println("Classe non trouvée lors de la désérialisation.");
    //         e.printStackTrace();
    //     }

    //     return map;
    // }

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

    public void WriteNewhashmapToS3(HashMap<String, List<List<String>>> map, String bucketName, String outputKey) {
        try {
            // Create output stream for serialized object
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            // Write serialized object to output stream
            objectOutputStream.writeObject(map);
            objectOutputStream.close();

            // Convert output stream to byte array
            byte[] bytes = byteArrayOutputStream.toByteArray();

            // Create upload request
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(outputKey) // Use the provided outputKey instead of hardcoded "hashmap.ser"
                    .build();

            // Upload serialized object to S3
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));

            System.out.println("Successfully uploaded batch file to S3: " + outputKey);
        } catch (S3Exception e) {
            System.err.println("S3 Error: " + e.awsErrorDetails().errorMessage());
            throw new RuntimeException("Failed to write to S3", e);
        } catch (IOException e) {
            System.err.println("IO Error during object serialization.");
            throw new RuntimeException("Failed to serialize map", e);
        }
    }
}