package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SplunkSplit implements InputSplit{

    private String jobID;
    private long start;
    private long end;
    private boolean skipHeader;

    public SplunkSplit() {
    }

    public SplunkSplit(long start, long end) {
        this("NONE",start,end);
    }

    public SplunkSplit(String jobID, long start, long end) {
        this(jobID, start, end, false);
    }

    public SplunkSplit(String jobID, long start, long end, boolean skipHeader) {
        this.start = start;
        this.end = end;
        this.jobID = jobID;
        this.skipHeader = skipHeader;
    }

    @Override
    public long getLength() throws IOException {
        return end-start;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[] {String.valueOf(start), String.valueOf(end)};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(jobID);
        dataOutput.writeLong(start);
        dataOutput.writeLong(end);
        dataOutput.writeBoolean(skipHeader);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.jobID = dataInput.readUTF();
        this.start = dataInput.readLong();
        this.end = dataInput.readLong();
        this.skipHeader = dataInput.readBoolean();
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    public String getJobID() {
        return this.jobID;
    }

    public boolean getSkipHeader() {
        return skipHeader;
    }
}
