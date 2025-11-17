package org.example;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    private static final String FOLDER_PATH = "/root/cc_ch";
    private static final int BATCH_SIZE = 100;

    public static void main(String[] args) throws Exception {
        List<Path> allFiles;
        try (Stream<Path> stream = Files.list(Paths.get(FOLDER_PATH))) {
            allFiles = stream
                    .filter(Files::isRegularFile)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.err.println("Failed to list files in folder: " + FOLDER_PATH);
            System.exit(1);
            return;
        }

        System.out.printf("Found %d files.%n", allFiles.size());

        List<List<Path>> batches = splitIntoBatches(allFiles);
        System.out.printf("Processing %d batches of up to %d files each.%n", batches.size(), BATCH_SIZE);

        int batchNum = 1;
        for (List<Path> batch : batches) {
            System.out.printf("%n=== Starting batch %d/%d (%d files) ===%n", batchNum, batches.size(), batch.size());
            ClickhousePipeline.runBatch(batch);
            System.out.printf("✅ Finished batch %d%n", batchNum);
            batchNum++;
        }

        System.out.println("\n✅ All batches completed.");
    }

    private static List<List<Path>> splitIntoBatches(List<Path> files) {
        List<List<Path>> result = new ArrayList<>();
        for (int i = 0; i < files.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, files.size());
            result.add(files.subList(i, end));
        }
        return result;
    }
}
