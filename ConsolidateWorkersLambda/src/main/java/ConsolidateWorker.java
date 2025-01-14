
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    private static final String HASHMAP_FILENAME = "hashmap.ser";

    public ConsolidateWorker(String outputBucket) {
        this.s3Client = S3Client.builder().build();
        this.outputBucket = outputBucket;
    }

    public String run(String sourceBucket, String sourceKey) {
        try {
            // Get the existing hashmap or create a new one
            HashMap<String, List<List<String>>> consolidateMap = ReadConsolidateMapFromS3(outputBucket);

            // Process the incoming CSV file
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3ObjectResponse = s3Client.getObject(request)) {
                processCsv(s3ObjectResponse, consolidateMap);
            }

            // Write updated hashmap back to S3
            WriteNewhashmapToS3(consolidateMap, outputBucket);

            return "Successfully processed " + sourceKey;
        } catch (Exception e) {
            throw new RuntimeException("Error processing S3 object: " + e.getMessage(), e);
        }
    }

    public void processCsv(InputStream inputStream, HashMap<String, List<List<String>>> consolidateMap)
            throws IOException, CsvException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)); CSVReader csvReader = new CSVReader(reader)) {

            List<String[]> records = csvReader.readAll();
            for (int i = 1; i < records.size(); i++) {
                String[] record = records.get(i);
                processRecord(record, consolidateMap);
            }
        }
    }

    private void processRecord(String[] record, HashMap<String, List<List<String>>> consolidateMap) {
        String sourceIp = record[1];
        String destIp = record[2];

        if (sourceIp.isEmpty() || destIp.isEmpty()) {
            return;
        }

        String key = String.format("%s,%s", sourceIp, destIp);
        String flowDuration = record[3];
        String forwardPackets = record[4];
        String date = record[0];

        consolidateMap.putIfAbsent(key, new ArrayList<>());
        List<List<String>> existingValues = consolidateMap.get(key);

        // Check if there's already an entry with the same date
        boolean dateFound = false;
        for (List<String> existingValue : existingValues) {
            if (existingValue.get(0).equals(date)) {
                // Sum up the values for matching dates
                long sumFlowDuration = Long.parseLong(existingValue.get(1)) + Long.parseLong(flowDuration);
                long sumForwardPackets = Long.parseLong(existingValue.get(2)) + Long.parseLong(forwardPackets);

                // Update the existing entry with summed values
                existingValue.set(1, String.valueOf(sumFlowDuration));
                existingValue.set(2, String.valueOf(sumForwardPackets));
                dateFound = true;
                break;
            }
        }

        // If no matching date was found, create new entry
        if (!dateFound) {
            List<String> newValue = new ArrayList<>();
            newValue.add(date);
            newValue.add(flowDuration);
            newValue.add(forwardPackets);
            existingValues.add(newValue);
            existingValues.sort((a, b) -> a.get(0).compareTo(b.get(0))); // Sort by date
        }
    }

    private HashMap<String, List<List<String>>> ReadConsolidateMapFromS3(String bucketName) {
        HashMap<String, List<List<String>>> map = null;

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
            try (InputStream inputStream = s3ObjectStream; ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {

                // Désérialiser l'objet
                Object obj = objectInputStream.readObject();
                if (obj instanceof HashMap) {
                    // Suppression des avertissements de type
                    @SuppressWarnings("unchecked")
                    HashMap<String, List<List<String>>> tempMap = (HashMap<String, List<List<String>>>) obj;
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

    public void WriteNewhashmapToS3(HashMap<String, List<List<String>>> map, String bucketName) {
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
}
