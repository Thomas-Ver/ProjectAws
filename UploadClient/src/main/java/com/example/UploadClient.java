package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class UploadClient {
  
  public static String bucketName = "rawdatalambda280825";
  public static Scanner scanner = new Scanner(System.in);
  
  public static void main(String[] args) {
    Region region = Region.US_EAST_1;
    S3Client s3 = S3Client.builder().region(region).build();
    
    System.out.println("Enter the directory path containing the files to upload: ");
    String directoryPath = scanner.nextLine().trim();
    
    File directory = new File(directoryPath);
    if (!directory.isDirectory()) {
      System.out.println("Invalid directory path!");
      return;
    }
    
    File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
    if (files == null || files.length == 0) {
      System.out.println("No CSV files found in the directory!");
      return;
    }
    
    System.out.println("Found " + files.length + " CSV files. Starting upload...");
    
    for (File file : files) {
      try {
        uploadFile(s3, file);
        System.out.println("Successfully uploaded: " + file.getName());
      } catch (Exception e) {
        System.out.println("Failed to upload " + file.getName() + ": " + e.getMessage());
      }
    }
    
    System.out.println("Upload process completed!");
  }
  
  private static void uploadFile(S3Client s3, File file) {
    PutObjectRequest putOb = PutObjectRequest.builder()
      .bucket(bucketName)
      .key(file.getName())
      .build();
    
    s3.putObject(putOb, RequestBody.fromBytes(getObjectFile(file.getAbsolutePath())));
  }
  
  private static byte[] getObjectFile(String filePath) {
    FileInputStream fileInputStream = null;
    byte[] bytesArray = null;
    
    try {
      File file = new File(filePath);
      bytesArray = new byte[(int) file.length()];
      fileInputStream = new FileInputStream(file);
      fileInputStream.read(bytesArray);
      
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    return bytesArray;
  }
}