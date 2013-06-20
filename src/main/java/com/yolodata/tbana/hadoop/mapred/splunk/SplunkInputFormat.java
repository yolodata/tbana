package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.ExportRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.SplunkRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplitProvider;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class SplunkInputFormat implements InputFormat<LongWritable, List<Text>> {

    public static final String INPUTFORMAT_SPLITS = "splunk.inputformat.splits";
    public static final String INPUTFORMAT_MODE = "splunk.inputformat.mode";

    public static enum Mode { Job, Export, Indexer, TwoLayerIndexer};
    private static final Mode DEFAULT_MODE = Mode.Job;

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
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

    private Mode getMethodFromConf(JobConf conf) {
        String mode = conf.get(INPUTFORMAT_MODE,"Job");

        for(Mode m : Mode.values())
            if(m.toString().equalsIgnoreCase(mode))
                return m;

        return DEFAULT_MODE;
    }

    private SplunkRecordReader getRecordReaderFromConf(JobConf conf) throws IOException {
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
