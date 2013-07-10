package com.yolodata.tbana.cascading.splunk;

import java.io.Serializable;

public class SplunkDataQuery implements Serializable {

    public static final String INDEX_LIST_SEPARATOR = ",";

    private String earliestTime;
    private String latestTime;
    private String[] indexes;
    public static final String[] ALL_INDEXES= new String[] {"*"};
    public static final String[] ALL_INDEXES_INCLUDING_INTERNAL= new String[] {"*", "_*"};

    public SplunkDataQuery(){
        this("0", "now");
    }

    public SplunkDataQuery(String earliestTime, String latestTime) {
        this(earliestTime, latestTime, ALL_INDEXES);
    }

    public SplunkDataQuery(String earliestTime, String latestTime, String[] indexes) {
        if(indexes == null)
            throw new NullPointerException();
        if(indexes.length == 0)
            throw new IllegalArgumentException("Empty index list is not allowed." +
                    " Use different constructor or SplunkDataQuery.ALL_INDEXES");
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
        this.indexes= indexes;
    }

    public String getSplunkQuery() {
        return "search index=" + concatenateIndexes(" OR index=");
    }

    private String concatenateIndexes(String separator) {
        StringBuilder results= new StringBuilder(indexes[0]);
        for(int i= 1; i < indexes.length; i++){
            results.append(separator + indexes[i]);
        }
        return results.toString();
    }

    public String getEarliestTime() {
        return earliestTime;
    }

    public String getLatestTime() {
        return latestTime;
    }

    public String [] getIndexes() {
        return indexes;
    }

    public String getIndexesString() {
        return concatenateIndexes(INDEX_LIST_SEPARATOR);
    }
}
