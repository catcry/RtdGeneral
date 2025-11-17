package org.example;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    private static final String FOLDER_PATH = "/root/cc_ch";

    public static void main(String[] args) throws Exception {
        Client client = new Client.Builder()
                .addEndpoint("http://192.168.5.197:8123")
                .setUsername("default")
                .setPassword("123123")
                .setDefaultDatabase("rtd")
                .build();

        List<Path> files;
        try (Stream<Path> s = Files.list(Paths.get(FOLDER_PATH))) {
            files = s.filter(Files::isRegularFile).collect(Collectors.toList());
        }

        System.out.println("Reading " + files.size() + " files...");

        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<List<ProcessedData>>> futures = new ArrayList<>();

        for (Path file : files) {
            futures.add(pool.submit(() -> {
                String content = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
                return Parser.parseFile(content, file.getFileName().toString());
            }));
        }

        List<ProcessedData> allData = new ArrayList<>();
        for (Future<List<ProcessedData>> f : futures) {
            allData.addAll(f.get());
        }
        pool.shutdown();

        System.out.println("Parsed " + allData.size() + " records. Inserting...");

        int batchSize = 10_000;
        for (int i = 0; i < allData.size(); i += batchSize) {
            int end = Math.min(i + batchSize, allData.size());
            List<ProcessedData> batch = allData.subList(i, end);

            InsertResponse resp = client.insert("hamid_processed_data")
                    .columns("file_name", "msisdn", "timestamp", "cat_list", "p_dis_list", "trigger_list")
                    .values(batch.stream()
                            .map(r -> Arrays.asList(
                                    r.getFileName(),
                                    r.getMsisdn(),
                                    r.getTimestamp(),
                                    r.getCatList(),
                                    r.getPDisList(),
                                    r.getTriggerList()
                            )).collect(Collectors.toList()))
                    .execute();

            System.out.println("Inserted batch of " + resp.getSummary().getWrittenRows());
        }
        try (InputStream dataStream = getDataStream()) {
            try (InsertResponse response = client.insert(TABLE_NAME, dataStream, ClickHouseFormat.JSONEachRow,
                    insertSettings).get(3, TimeUnit.SECONDS)) {

                log.info("Insert finished: {} rows written", response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
            } catch (Exception e) {
                log.error("Failed to write JSONEachRow data", e);
                throw new RuntimeException(e);
            }
        }

        client.close();
        System.out.println("✅ All done!");
    }
}

