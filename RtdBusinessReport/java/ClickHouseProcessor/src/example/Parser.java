package org.example;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static List<ProcessedData> parseFile(String data, String fileName) {
        List<ProcessedData> result = new ArrayList<>();
        int start = data.indexOf("RECORD");
        if (start < 0) return result;

        while (true) {
            int next = data.indexOf("RECORD", start + 1);
            String record = (next >= 0) ? data.substring(start, next) : data.substring(start);

            String msisdn = getField("callingNumber", record);
            String tsStr = getField("eventStartTimestamp", record);

            if (msisdn != null && tsStr != null) {
                try {
                    LocalDateTime dt = LocalDateTime.parse(tsStr, TS_FMT);
                    long timestamp = dt.toEpochSecond(java.time.ZoneOffset.UTC);

                    List<String> cat = new ArrayList<>();
                    List<String> pdis = new ArrayList<>();
                    List<String> trigger = new ArrayList<>();

                    int idx = 0;
                    while (true) {
                        int blockStart = record.indexOf("B ", idx);
                        if (blockStart < 0) break;
                        int blockEnd = record.indexOf("\n", blockStart);
                        if (blockEnd < 0) blockEnd = record.length();
                        String blockLine = record.substring(blockStart, blockEnd);

                        if (blockLine.contains("__CATEGORIZATION__")) {
                            int fIdx = record.indexOf("FAILED_LOGICS", blockStart);
                            if (fIdx > 0) {
                                int fEnd = record.indexOf('.', fIdx);
                                String section = record.substring(fIdx, fEnd > 0 ? fEnd : record.length());
                                cat.addAll(getBlockKeys(section));
                            }
                        } else if (blockLine.contains("__RULE_ENGINE__")) {
                            int fIdx = record.indexOf("TRIGGERED_ACTIONS", blockStart);
                            if (fIdx > 0) {
                                int fEnd = record.indexOf('.', fIdx);
                                String section = record.substring(fIdx, fEnd > 0 ? fEnd : record.length());
                                pdis.addAll(getBlockKeys(section));
                            }
                        } else if (blockLine.contains("__ACTION_MONITOR__")) {
                            String section = record.substring(blockStart);
                            trigger.addAll(getActionMonitorLogicIds(section));
                        }

                        idx = blockEnd;
                    }

                    result.add(new ProcessedData(fileName, msisdn, timestamp, cat, pdis, trigger));
                } catch (Exception ignored) {}
            }

            if (next < 0) break;
            start = next;
        }
        return result;
    }

    private static String getField(String name, String data) {
        int idx = data.indexOf(name);
        if (idx < 0) return null;
        int end = data.indexOf("\n", idx);
        if (end < 0) end = data.length();
        return data.substring(idx + name.length()).trim();
    }

    private static List<String> getBlockKeys(String data) {
        List<String> keys = new ArrayList<>();
        for (String line : data.split("\n")) {
            if (line.startsWith("F ")) {
                String[] parts = line.split(" ");
                if (parts.length > 1) keys.add(parts[1]);
            }
        }
        return keys;
    }

    private static List<String> getActionMonitorLogicIds(String data) {
        List<String> ids = new ArrayList<>();
        Pattern pattern = Pattern.compile("F ID (\\S+)");
        Matcher m = pattern.matcher(data);
        while (m.find()) ids.add(m.group(1));
        return ids;
    }
}

