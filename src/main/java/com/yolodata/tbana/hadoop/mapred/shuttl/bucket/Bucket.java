package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

public class Bucket {

    private String name;
    private final long start;
    private final long end;

    public Bucket(String name, long start, long end) {
        this.name = name;
        this.start = start;
        this.end = end;
    }

    public static Bucket create(String rawName) {
        String [] rawFields = rawName.split("_");

        return new Bucket(rawFields[0],Long.parseLong(rawFields[1]),Long.parseLong(rawFields[2]));
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }
}
