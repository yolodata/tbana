package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;


import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.splunk.*;

public abstract class SplunkRecordReader implements RecordReader<LongWritable, List<Text>> {

    protected final JobConf configuration;
    protected long currentPosition;
    protected long startPosition;
    protected long endPosition;
    protected Service splunkService;

    protected InputStream is;
    protected InputStreamReader in;
    protected CSVReader reader;

    public SplunkRecordReader(JobConf configuration) throws IOException {

        this.configuration = configuration;
        SplunkConf.validateConfiguration(this.configuration);
        splunkService = SplunkService.connect(configuration);

    }

    public abstract void initialize(InputSplit inputSplit) throws IOException;

    protected void initPositions(SplunkSplit inputSplit) {
        startPosition = inputSplit.getStart();
        endPosition = inputSplit.getEnd();
        currentPosition = startPosition;
    }

    @Override
    public boolean next(LongWritable key, List<Text> value) throws IOException {

        reader = new CSVReader(in);
        if(key == null) key = createKey();
        if(value == null) value = createValue();

        int bytesRead = reader.readLine(value);

        if(bytesRead == 0) {
            key = null;
            value = null;
            return false;
        }

        key.set(currentPosition++);
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public List<Text> createValue() {
        return new ArrayListTextWritable();
    }

    @Override
    public long getPos() throws IOException {
        return currentPosition;
    }

    @Override
    public void close() throws IOException {
        if(is!=null) {
            is.close();
            is=null;
        }

        if(in!=null) {
            in.close();
            in=null;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (startPosition == endPosition) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPosition - startPosition) / (float) (endPosition - startPosition));
        }
    }

}
