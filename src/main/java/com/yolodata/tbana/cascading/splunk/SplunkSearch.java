package com.yolodata.tbana.cascading.splunk;

import cascading.tuple.Fields;

import java.io.Serializable;

public class SplunkSearch implements Serializable {

    private String query;
    private String earliestTime;
    private String latestTime;
    private Fields fields;

    public SplunkSearch(String query, String earliestTime, String latestTime) {
        this(query, earliestTime, latestTime, Fields.ALL);
    }

    public SplunkSearch(String query, String earliestTime, String latestTime, Fields fields) {
        this.query = query;
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
        this.fields = fields;
    }

    public String getQuery() {
        return query;
    }

    public String getEarliestTime() {
        return earliestTime;
    }

    public String getLatestTime() {
        return latestTime;
    }

    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields newFields) {
        fields = newFields;
    }
}
