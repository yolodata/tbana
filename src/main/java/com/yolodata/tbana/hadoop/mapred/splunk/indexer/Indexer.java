package com.yolodata.tbana.hadoop.mapred.splunk.indexer;

public class Indexer {
    private String host;
    private int port;

    public Indexer(String host, int port){
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
