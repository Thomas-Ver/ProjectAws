
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
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

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
                System.out.println("Key was found in the HashMap.");
                List<List<String>> rawData = Consolidatemap.get(key);
                List<List<String>> consolidatedData = consolidateDates(rawData);
                CreatCsvFile(consolidatedData, sourceIP, destinationIP);
            } else {
                System.out.println("The key doesn't exist in the HashMap.");
            }
        }
    }

    public HashMap<String, List<List<String>>> ReadConsolidateMapFromS3(String bucketName, S3Client s3Client) {
        HashMap<String, List<List<String>>> map = null;

        try {
            if (!fileExistsOnS3(s3Client, bucketName, "hashmap.ser")) {
                return new HashMap<>();
            }
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key("hashmap.ser")
                    .build();

            ResponseInputStream<?> s3ObjectStream = s3Client.getObject(getObjectRequest);

            try (ResponseInputStream<?> inputStream = s3ObjectStream; ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
                Object obj = objectInputStream.readObject();
                if (obj instanceof HashMap) {
                    @SuppressWarnings("unchecked")
                    HashMap<String, List<List<String>>> tempMap = (HashMap<String, List<List<String>>>) obj;
                    map = tempMap;
                }
            }
        } catch (S3Exception | IOException | ClassNotFoundException e) {
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
                return false;
            } else {
                throw e;
            }
        }
    }

    public static List<List<String>> consolidateDates(List<List<String>> data) {
        HashMap<String, List<Double>> dateMap = new HashMap<>();

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

        List<List<String>> consolidatedData = new ArrayList<>();
        for (String date : dateMap.keySet()) {
            List<Double> values = dateMap.get(date);
            List<String> consolidatedRecord = new ArrayList<>();
            consolidatedRecord.add(date);
            consolidatedRecord.add(String.valueOf(values.get(0)));
            consolidatedRecord.add(String.valueOf(Math.round(values.get(1))));
            consolidatedData.add(consolidatedRecord);
        }

        consolidatedData.sort((a, b) -> a.get(0).compareTo(b.get(0)));
        return consolidatedData;
    }

    public static void CreatCsvFile(List<List<String>> data, String sourceIp, String destinationIp) {
        List<Double> ListMeanAndVariance = MeanAndVariance(data);
        try {
            FileWriter writer = new FileWriter("data_" + sourceIp + ":" + destinationIp + ".csv");
            writer.append("Source IP,Destination IP,Mean TotalFlowDuration,Standard Deviation TotalFlowDuration,Mean TotalPacketsForward,Standard Deviation TotalPacketsForward\n");
            writer.append(sourceIp).append(",").append(destinationIp).append(",")
                    .append(ListMeanAndVariance.get(0).toString()).append(",")
                    .append(ListMeanAndVariance.get(1).toString()).append(",")
                    .append(ListMeanAndVariance.get(2).toString()).append(",")
                    .append(ListMeanAndVariance.get(3).toString()).append("\n\n");
            writer.append("Date,TotalFlowDuration,TotalPacketsForward\n");
            for (List<String> L : data) {
                writer.append(String.join(",", L)).append("\n");
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
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
        MeanTfdPerday /= data.size();
        MeanTfpPerday /= data.size();
        for (List<String> L : data) {
            VarianceTfdPerday += Math.pow(Double.parseDouble(L.get(1)) - MeanTfdPerday, 2);
            VarianceTfpPerday += Math.pow(Double.parseDouble(L.get(2)) - MeanTfpPerday, 2);
        }
        VarianceTfdPerday /= data.size();
        VarianceTfpPerday /= data.size();
        ListMeanAndVariance.add(MeanTfdPerday);
        ListMeanAndVariance.add(Math.sqrt(VarianceTfdPerday));
        ListMeanAndVariance.add(MeanTfpPerday);
        ListMeanAndVariance.add(Math.sqrt(VarianceTfpPerday));
        return ListMeanAndVariance;
    }
}
