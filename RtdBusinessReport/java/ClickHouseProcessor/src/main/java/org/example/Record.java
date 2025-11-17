package org.example;

import java.time.LocalDateTime;
import java.util.List;

public class Record {
    private String file_name;
    private String msisdn;
    private LocalDateTime timestamp;
    private List<String> cat_list;
    private List<String> p_dis_list;
    private List<String> trigger_list;

    public Record(String file_name, String msisdn, LocalDateTime timestamp,
                       List<String> cat_list, List<String> p_dis_list, List<String> trigger_list) {
        this.file_name = file_name;
        this.msisdn = msisdn;
        this.timestamp = timestamp;
        this.cat_list = cat_list;
        this.p_dis_list = p_dis_list;
        this.trigger_list = trigger_list;
    }
    public String getFile_name() {return file_name;}
    public String getMsisdn() {return msisdn;}
    public LocalDateTime getTimestamp() {return timestamp;}
    public List<String> getCat_list() {return cat_list;}
    public List<String> getP_dis_list() {return p_dis_list;}
    public List<String> getTrigger_list() {return trigger_list;}
    public void setFile_name(String file_name) {this.file_name = file_name;}
    public void setMsisdn(String msisdn) {this.msisdn = msisdn;}
    public void setTimestamp(LocalDateTime timestamp) {this.timestamp = timestamp;}
    public void setCat_list(List<String> cat_list) {this.cat_list = cat_list;}
    public void setP_dis_list(List<String> p_dis_list) {this.p_dis_list = p_dis_list;}
    public void setTrigger_list(List<String> trigger_list) {this.trigger_list = trigger_list;}

}

