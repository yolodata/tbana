package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.ExportRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.SplunkRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplitProvider;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class SplunkInputFormat implements InputFormat<LongWritable, ArrayListTextWritable> {

    public static final String INPUTFORMAT_SPLITS = "splunk.inputformat.splits";
    public static final String INPUTFORMAT_MODE = "splunk.inputformat.mode";

    public static enum Mode { Job, Export, Indexer, TwoLayerIndexer};
    private static final Mode DEFAULT_MODE = Mode.Job;

    @Override
    public RecordReader<LongWritable,ArrayListTextWritable> getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
        SplunkRecordReader splunkRecordReader = getRecordReaderFromConf(configuration);
        splunkRecordReader.initialize(inputSplit);

        return splunkRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int i) throws IOException {

        Mode mode = getMethodFromConf(conf);
        SplitProvider sp = SplitProvider.getProvider(mode);

        return sp.getSplits(conf,i);
    }

    private static Mode getMethodFromConf(Configuration conf) {
        String mode = conf.get(INPUTFORMAT_MODE,"Job");

        for(Mode m : Mode.values())
            if(m.toString().equalsIgnoreCase(mode))
                return m;

        return DEFAULT_MODE;
    }

    public static SplunkRecordReader getRecordReaderFromConf(Configuration conf) throws IOException {
        Mode mode = getMethodFromConf(conf);
        switch(mode) {
            case Job:
                return new JobRecordReader(conf);
            case Export:
                return new ExportRecordReader(conf);
            case Indexer:
                return new JobRecordReader(conf);
            default:
                throw new RuntimeException("Cannot get SplunkRecordReader in SplunkInputFormat using mode "+ mode.toString());
        }

    }
}
