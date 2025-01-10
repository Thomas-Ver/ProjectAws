import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;


import java.io.*;
import java.util.*;

public class ProcessCsvTest {

  private final String outputBucket;
  private Map<String, AggregatedData> ConsolidateMap = new HashMap<>();

  public ProcessCsvTest(String outputBucket, Map<String, AggregatedData> ConsolidateMap) {
    this.outputBucket = outputBucket;
    this.ConsolidateMap = ConsolidateMap;
  }

  public static void main(String[] args) {
    ProcessCsvTest processCsvTest = new ProcessCsvTest("tmor", new HashMap<>());
    String pathFile = "/mnt/c/Users/thoma/OneDrive/Bureau/3A_ICM/majeur_info/Cloud_and_edge/FinalProject/ConsolidateWorkers/daily_summary_2025-01-01_data-20221207.csv";
    try {
      processCsvTest.processCsv(pathFile);
      processCsvTest.testReadhashfile();
    } catch (Exception e) {
      System.err.println("Error processing message: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public void processCsv(String path) throws IOException, CsvException {
    Map<String, AggregatedData> ConsolidateMap = new HashMap<>();

    try (CSVReader csvReader = new CSVReader(new FileReader(path))) {

      // On liste toutes les lignes ici
      List<String[]> records = csvReader.readAll();

      // on va sur chaque ligne en esquivant la premiére
      for (int i = 1; i < records.size(); i++) {
        String[] record = records.get(i);
        // record[0] correspond à la ligne i 1er colone (ici la date)
        processRecord(record, ConsolidateMap);

      }
      
      convertToOutput(ConsolidateMap);
    }
  }

  private void processRecord(String[] record, Map<String,AggregatedData> ConsolidateMap) {

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

  
    private void convertToOutput(Map<String,AggregatedData> ConsolidateMap) {

    // on va stocker la hashmap dans un fichier
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("hashmap.ser"))) {
      oos.writeObject(ConsolidateMap);
      System.out.println("HashMap sauvegardée dans hashmap.ser");
  } catch (IOException e) {
      e.printStackTrace();
  }
}

    private void testReadhashfile() {
      // Chargement de la HashMap depuis un fichier binaire
      try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream("hashmap.ser"))) {
        HashMap<String, AggregatedData> loadedMap = (HashMap<String, AggregatedData>)ois.readObject();
        System.out.println("HashMap chargée : " );
        System.out.println("MeanTfdPerday : " + loadedMap.get("192.168.20.48,15.188.36.183").MeanTfdPerday);
        loadedMap.get("192.168.20.48,15.188.36.183").add(new ArrayList<>(Arrays.asList("2025-01-01", "100", "200")));
        System.out.println("MeanTfdPerday : " + loadedMap.get("192.168.20.48,15.188.36.183").MeanTfdPerday);
    } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
  
  
    }
   

}
}
