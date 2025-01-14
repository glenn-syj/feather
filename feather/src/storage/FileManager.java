package storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Temporary FileManager class for the file system based database.
 * The first version works on a single thread and a single node.
 */
public class FileManager {
    private static final String STORAGE_DIR = "data/";

    public FileManager() {
        try {
            Files.createDirectories(Paths.get(STORAGE_DIR));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directory", e);
        }
    }

    public void saveDocument(String index, int id, String content) throws IOException {
        Path filePath = Paths.get(STORAGE_DIR, index + ".dat");
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(id + ":" + content);
            writer.newLine();
        }
    }

    public List<String> loadDocuments(String index, List<Integer> documentIds) {
        List<String> results = new ArrayList<>();
        Path filePath = Paths.get(STORAGE_DIR, index + ".dat");

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":", 2);
                int id = Integer.parseInt(parts[0]);
                if (documentIds.contains(id)) {
                    results.add(parts[1]);
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load documents: " + e.getMessage());
        }

        return results;
    }
}