package org.example;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.*;

public class TransformV3 {

    // ================== Utility Functions ==================

    public static String getField(String fieldName, String data) {
        int idx = data.indexOf(fieldName);
        if (idx == -1) return "";
        int end = data.indexOf('\n', idx);
        if (end == -1) end = data.length();
        return data.substring(idx + fieldName.length(), end).trim();
    }

    public static List<String> getSimpleBlock(String blockName, String data) {
        String pattern = Pattern.quote(blockName) + "(.*?)\\.";
        Matcher matcher = Pattern.compile(pattern, Pattern.DOTALL).matcher(data);
        if (!matcher.find()) return Collections.emptyList();

        String block = matcher.group(1).trim();
        List<String> ids = new ArrayList<>();
        for (String line : block.split("\\R")) {
            if (line.startsWith("F ")) {
                String[] parts = line.split(" ", 3);
                if (parts.length > 1) ids.add(parts[1]);
            }
        }
        return ids;
    }
    private static final Pattern logic_patter = Pattern.compile("B LOGIC.*?F ID ([^\\n]*)", Pattern.DOTALL);

    public static List<String> getActionMonitor(String amData) {
        Matcher matcher = logic_patter.matcher(amData);
        List<String> ids = new ArrayList<>();
        while (matcher.find()) {
            ids.add(matcher.group(1).trim());
        }
        return ids;
    }

    // ================== Record Parsing ==================

    public static Optional<Record> parseBlock(String fileName, String data) {
        if (!data.contains("callingNumber") || !data.contains("eventStartTimestamp")) {
            return Optional.empty();
        }

        try {
            String msisdn = getField("callingNumber", data);
            String tsStr = getField("eventStartTimestamp", data);
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            LocalDateTime timestamp = LocalDateTime.parse(tsStr, fmt);

            int categoryIdx = data.indexOf("B __CATEGORIZATION__");
            int ruleIdx = data.indexOf("B __RULE_ENGINE__");
            int actionIdx = data.indexOf("B __ACTION_MONITOR__");
            int dataLen = data.length();

            int nextCategoryEnd = nextIndex(ruleIdx, actionIdx, dataLen);
            int nextRuleEnd = nextIndex(actionIdx, dataLen);

            List<String> catList = categoryIdx != -1 ? getSimpleBlock("B FAILED_LOGICS", data.substring(categoryIdx, nextCategoryEnd)) : new ArrayList<>();
            List<String> triggerList = ruleIdx != -1 ? getSimpleBlock("B TRIGGERED_ACTIONS", data.substring(ruleIdx, nextRuleEnd)) : new ArrayList<>();
            List<String> pDisList = actionIdx != -1 ? getActionMonitor(data.substring(actionIdx)) : new ArrayList<>();

            return Optional.of(new Record(fileName, msisdn, timestamp, catList, pDisList, triggerList));

        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static int nextIndex(int... indices) {
        return Arrays.stream(indices).filter(i -> i != -1).min().orElse(Integer.MAX_VALUE);
    }
    private static final Pattern recordPattern = Pattern.compile("RECORD");
    public static List<Record> parseRecords(String fileName, String data) {
        List<Record> totalRecords = new ArrayList<>();
        Matcher matcher = recordPattern.matcher(data);

        List<Integer> indices = new ArrayList<>();
        while (matcher.find()) indices.add(matcher.start());
        indices.add(data.length());

        for (int i = 0; i < indices.size() - 1; i++) {
            String recordData = data.substring(indices.get(i), indices.get(i + 1));
            parseBlock(fileName, recordData).ifPresent(totalRecords::add);
        }

        return totalRecords;
    }

    // ================== File Processing ==================

    public static List<Record> mainProcess(Path filePath, String data) {
        String fileName = filePath.getFileName().toString();
        try {
            return parseRecords(fileName, data);
        } catch (Exception e) {
            System.err.println("Error processing " + fileName + ": " + e.getMessage());
            return Collections.emptyList();
        }
    }

}

