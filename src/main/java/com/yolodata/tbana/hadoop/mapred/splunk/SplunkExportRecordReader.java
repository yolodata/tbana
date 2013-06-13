package com.yolodata.tbana.hadoop.mapred.splunk;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.List;

public class SplunkExportRecordReader implements RecordReader<LongWritable, List<Text>> {

    @Override
    public boolean next(LongWritable longWritable, List<Text> texts) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongWritable createKey() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Text> createValue() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getPos() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getProgress() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
