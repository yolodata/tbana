package com.yolodata.tbana.hadoop.mapred.shuttl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CsvSplit implements InputSplit {

    protected long start;
    protected long length;
    protected long keyStart;

    private boolean skipHeader;
    private Path filepath;

    public CsvSplit() {

    }

    public CsvSplit(Path filepath, long start, long length) {
        this(filepath, start, length, 0, false);
    }

    public long getStart() {
        return start;
    }

    public long getKeyStart() {
        return keyStart;
    }

    public boolean isSkipHeader() {
        return skipHeader;
    }

    public Path getFilepath() {
        return filepath;
    }

    public CsvSplit(Path filepath, long start, long length, long keyStart, boolean skipHeader) {

        this.filepath = filepath;
        this.start = start;
        this.length = length;
        this.keyStart = keyStart;
        this.skipHeader = skipHeader;
    }

    public void setSkipHeader(boolean value) {
        skipHeader = value;
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[] {String.valueOf(start), String.valueOf(length)};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(filepath.toString());
        dataOutput.writeLong(start);
        dataOutput.writeLong(length);
        dataOutput.writeLong(keyStart);
        dataOutput.writeBoolean(skipHeader);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        filepath = new Path(dataInput.readUTF());
        start = dataInput.readLong();
        length = dataInput.readLong();
        keyStart = dataInput.readLong();
        skipHeader = dataInput.readBoolean();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass() != getClass())
            return false;

        CsvSplit split = (CsvSplit) obj;
        return this.filepath.equals(split.filepath) &&
                this.start == split.start &&
                this.length == split.length &&
                this.keyStart == split.keyStart &&
                this.skipHeader == split.skipHeader;
    }
}
