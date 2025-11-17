package org.example;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.enums.Protocol;
import com.clickhouse.client.api.insert.InsertResponse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ClickhousePipeline {

    private static final String CLICKHOUSE_HOST = "192.168.5.197";
    private static final int CLICKHOUSE_PORT = 8123;
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASS = "123123";
    private static final String CLICKHOUSE_DB = "rtd";
    private static final String TABLE_NAME = "hamid_processed_data";

    public static void runBatch(List<Path> filePaths) throws Exception {
        System.out.printf("Processing batch of %d files...%n", filePaths.size());

        // Step 1: Read files asynchronously
        ExecutorService ioPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<CompletableFuture<FileData>> fileFutures = filePaths.stream()
                .map(path -> CompletableFuture.supplyAsync(() -> readFile(path), ioPool))
                .collect(Collectors.toList());

        List<FileData> fileDataList = fileFutures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        ioPool.shutdown();

        // Step 2: Parse in parallel
        ForkJoinPool cpuPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        List<Record> processedData = cpuPool.submit(() ->
                fileDataList.parallelStream()
                        .flatMap(fd -> TransformV3.mainProcess(fd.filePath, fd.content).stream())
                        .collect(Collectors.toList())
        ).get();
        cpuPool.shutdown();

        System.out.printf("Parsed %d records in this batch.%n", processedData.size());

        // Step 3: Insert into ClickHouse using non-deprecated API
        insertIntoClickHouse(processedData);
    }

    private static FileData readFile(Path filePath) {
        try {
            String data = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
            return new FileData(filePath, data);
        } catch (Exception e) {
            System.err.println("Failed to read " + filePath + ": " + e.getMessage());
            return null;
        }
    }

    private static void insertIntoClickHouse(List<Record> records) throws Exception {
        if (records.isEmpty()) return;

        // Create a node and request
        Client client = new Client.Builder()
                .addEndpoint(Protocol.HTTP,CLICKHOUSE_HOST,CLICKHOUSE_PORT,false)
                .setUsername(CLICKHOUSE_USER)
                .setPassword(CLICKHOUSE_PASS)
                .setDefaultDatabase(CLICKHOUSE_DB)
                .build();

        int batchSize = 50_000;
        Record r = records.get(0);
        System.out.println("record = " + r);
        client.register(Record.class, client.getTableSchema("hamid_processed_data"));

        for (int i = 0; i < records.size(); i += batchSize) {
            List<Record> dtos = records.subList(i, Math.min(i + batchSize, records.size()));
            try (InsertResponse response = client.insert("hamid_processed_data", dtos).get()) {
                System.out.println("Inserted " + dtos.size() + " records successfully.");
            }

        }
    }

    private static class FileData {
        final Path filePath;
        final String content;

        FileData(Path filePath, String content) {
            this.filePath = filePath;
            this.content = content;
        }
    }
}
