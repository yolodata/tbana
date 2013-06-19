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
    public static final String INPUTFORMAT_METHOD = "splunk.inputformat.method";

    public static enum Method { Job, Export, Indexer, TwoLayerIndexer};
    private static final Method DEFAULT_METHOD = Method.Job;

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
        SplunkRecordReader splunkRecordReader = getRecordReaderFromConf(configuration);
        splunkRecordReader.initialize(inputSplit);

        return splunkRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int i) throws IOException {

        Method method = getMethodFromConf(conf);
        SplitProvider sp = SplitProvider.getProvider(method);

        return sp.getSplits(conf,i);
    }

    private Method getMethodFromConf(JobConf conf) {
        String method = conf.get(INPUTFORMAT_METHOD,"Job");

        for(Method m : Method.values())
            if(m.toString().equalsIgnoreCase(method))
                return m;

        return DEFAULT_METHOD;
    }

    private SplunkRecordReader getRecordReaderFromConf(JobConf conf) throws IOException {
        Method method = getMethodFromConf(conf);
        switch(method) {
            case Job:
                return new JobRecordReader(conf);
            case Export:
                return new ExportRecordReader(conf);
            case Indexer:
                return new JobRecordReader(conf);
            default:
                throw new RuntimeException("Cannot get SplunkRecordReader in SplunkInputFormat using method "+method.toString());
        }

    }
}
