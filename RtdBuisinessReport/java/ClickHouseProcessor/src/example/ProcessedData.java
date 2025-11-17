package org.example;

import java.util.List;

public class ProcessedData {
    private String fileName;
    private String msisdn;
    private long timestamp;
    private List<String> catList;
    private List<String> pDisList;
    private List<String> triggerList;

    public ProcessedData(String fileName, String msisdn, long timestamp,
                         List<String> catList, List<String> pDisList, List<String> triggerList) {
        this.fileName = fileName;
        this.msisdn = msisdn;
        this.timestamp = timestamp;
        this.catList = catList;
        this.pDisList = pDisList;
        this.triggerList = triggerList;
    }

    public String getFileName() { return fileName; }
    public String getMsisdn() { return msisdn; }
    public long getTimestamp() { return timestamp; }
    public List<String> getCatList() { return catList; }
    public List<String> getPDisList() { return pDisList; }
    public List<String> getTriggerList() { return triggerList; }
}

