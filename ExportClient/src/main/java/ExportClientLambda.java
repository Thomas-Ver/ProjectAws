
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ExportClientLambda {

    public static String bucketName = "s3-consolidated-data-021095";
    public static String keyName = "hashmap.ser";
    public static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        boolean bool = true;
        S3Client s3 = S3Client.builder().build();
        HashMap<String, List<List<String>>> Consolidatemap = new ExportClientLambda().ReadConsolidateMapFromS3(bucketName, s3);
        while (bool) {

            System.out.println("source IP: ");
            String sourceIP = scanner.nextLine();
            System.out.println("destination IP: ");
            String destinationIP = scanner.nextLine();
            String key = sourceIP + "," + destinationIP;
            if (Consolidatemap.containsKey(key)) {
                System.out.println("La clé existe dans la HashMap");
                List<List<String>> rawData = Consolidatemap.get(key);
                List<List<String>> consolidatedData = consolidateDates(rawData);
                CreatCsvFile(consolidatedData, sourceIP, destinationIP);
            } else {
                System.out.println("La clé n'existe pas dans la HashMap");

            }
        }
    }

    public HashMap<String, List<List<String>>> ReadConsolidateMapFromS3(String bucketName, S3Client s3Client) {
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

    public static List<List<String>> consolidateDates(List<List<String>> data) {
        // Create a map to store consolidated values for each date
        HashMap<String, List<Double>> dateMap = new HashMap<>();

        // Group and sum values by date
        for (List<String> record : data) {
            String date = record.get(0);
            double flowDuration = Double.parseDouble(record.get(1));
            double packetCount = Double.parseDouble(record.get(2));

            dateMap.putIfAbsent(date, new ArrayList<>());
            List<Double> values = dateMap.get(date);
            if (values.isEmpty()) {
                values.add(flowDuration);
                values.add(packetCount);
            } else {
                values.set(0, values.get(0) + flowDuration);
                values.set(1, values.get(1) + packetCount);
            }
        }

        // Create consolidated list
        List<List<String>> consolidatedData = new ArrayList<>();
        for (String date : dateMap.keySet()) {
            List<Double> values = dateMap.get(date);

            List<String> consolidatedRecord = new ArrayList<>();
            consolidatedRecord.add(date);
            consolidatedRecord.add(String.valueOf(values.get(0))); // Total flow duration
            consolidatedRecord.add(String.valueOf(Math.round(values.get(1)))); // Total packets
            consolidatedData.add(consolidatedRecord);
        }

        // Sort by date
        consolidatedData.sort((a, b) -> a.get(0).compareTo(b.get(0)));

        return consolidatedData;
    }

    public static void CreatCsvFile(List<List<String>> data, String sourceIp, String destinationIp) {

        List<Double> ListMeanAndVariance = new ArrayList<>();
        ListMeanAndVariance = MeanAndVariance(data);
        try {
            FileWriter writer = new FileWriter("data_" + sourceIp + ":" + destinationIp + ".csv");
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
            writer.append(ListMeanAndVariance.get(0).toString());
            writer.append(",");
            writer.append(ListMeanAndVariance.get(1).toString());
            writer.append(",");
            writer.append(ListMeanAndVariance.get(2).toString());
            writer.append(",");
            writer.append(ListMeanAndVariance.get(3).toString());
            writer.append("\n");
            writer.append("\n");
            writer.append("Date");
            writer.append(",");
            writer.append("TotalFlowDuration");
            writer.append(",");
            writer.append("TotalPacketsForward");
            writer.append("\n");
            for (List<String> L : data) {
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

    public static List<Double> MeanAndVariance(List<List<String>> data) {
        List<Double> ListMeanAndVariance = new ArrayList<>();
        Double MeanTfdPerday = 0.0;
        Double MeanTfpPerday = 0.0;
        Double VarianceTfdPerday = 0.0;
        Double VarianceTfpPerday = 0.0;
        for (List<String> L : data) {
            MeanTfdPerday += Double.parseDouble(L.get(1));
            MeanTfpPerday += Double.parseDouble(L.get(2));
        }
        MeanTfdPerday = MeanTfdPerday / data.size();
        MeanTfpPerday = MeanTfpPerday / data.size();
        for (List<String> L : data) {
            VarianceTfdPerday += Math.pow(Double.parseDouble(L.get(1)) - MeanTfdPerday, 2);
            VarianceTfpPerday += Math.pow(Double.parseDouble(L.get(2)) - MeanTfpPerday, 2);
        }
        VarianceTfdPerday = VarianceTfdPerday / data.size();
        VarianceTfpPerday = VarianceTfpPerday / data.size();
        ListMeanAndVariance.add(MeanTfdPerday);
        ListMeanAndVariance.add(Math.sqrt(VarianceTfdPerday));
        ListMeanAndVariance.add(MeanTfpPerday);
        ListMeanAndVariance.add(Math.sqrt(VarianceTfpPerday));
        return ListMeanAndVariance;
    }
}
