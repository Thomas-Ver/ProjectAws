
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class ExportClientLambda {

    public static String bucketName = "s3-consolidated-data-lambda-021095";
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
        HashMap<String, List<List<String>>> mergedMap = new HashMap<>();

        try {
            // List all batch files in the batches/ prefix
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix("batches/")
                    .build();

            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);

            // Merge all batch files
            for (S3Object s3Object : listResponse.contents()) {
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Object.key())
                        .build();

                try (ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(getRequest); ObjectInputStream objectInputStream = new ObjectInputStream(s3ObjectStream)) {

                    @SuppressWarnings("unchecked")
                    HashMap<String, List<List<String>>> batchMap
                            = (HashMap<String, List<List<String>>>) objectInputStream.readObject();

                    // Merge this batch's data into the main map
                    mergeMaps(mergedMap, batchMap);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Error reading batch files", e);
        }

        return mergedMap;
    }

    private void mergeMaps(HashMap<String, List<List<String>>> target, HashMap<String, List<List<String>>> source) {
        source.forEach((key, sourceList) -> {
            List<List<String>> targetList = target.computeIfAbsent(key, k -> new ArrayList<>());
            for (List<String> record : sourceList) {
                // Add only if this exact combination doesn't exist
                boolean exists = targetList.stream().anyMatch(existing
                        -> existing.get(0).equals(record.get(0))
                        && existing.get(1).equals(record.get(1))
                        && existing.get(2).equals(record.get(2))
                );
                if (!exists) {
                    targetList.add(record);
                }
            }
        });
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
