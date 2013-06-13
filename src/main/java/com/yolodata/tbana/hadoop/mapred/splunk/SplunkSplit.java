package com.yolodata.tbana.hadoop.mapred.splunk;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SplunkSplit implements InputSplit{

    private String[] locations;
    private long start;
    private long end;

    private long length;

    public SplunkSplit(long start, long end) {
        locations = new String[] {String.valueOf(start), String.valueOf(end)};
    }

    @Override
    public long getLength() throws IOException {
        return end-start;
    }

    @Override
    public String[] getLocations() throws IOException {
        return locations;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new NotImplementedException();
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }
}
