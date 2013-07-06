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

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass() != getClass())
            return false;

        Indexer indexer = (Indexer) obj;
        return this.host.equals(indexer.host) &&
                this.port == indexer.port;
    }
}
