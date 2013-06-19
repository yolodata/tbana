package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;


public abstract class SplitProvider {

    public abstract InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException;

    public static SplitProvider getProvider(SplunkInputFormat.Method inputFormatMethod) {
        switch (inputFormatMethod) {
            case Job:
                return new JobSplitProvider();
            case Export:
                return new ExportSplitProvider();
            case Indexer:
                return new IndexerSplitProvider();
            default:
                throw new RuntimeException("SplitProvider for Method " + inputFormatMethod.toString() + " is not specified in SplitProvider.getProvider(Method)");
            case TwoLayerIndexer:
                break;
        }
        return null;
    }

    protected int getNumberOfSplits(JobConf conf, int defaultValue) {
        if(conf.get(SplunkInputFormat.INPUTFORMAT_SPLITS) != null)
            defaultValue = Integer.parseInt(conf.get(SplunkInputFormat.INPUTFORMAT_SPLITS));
        return defaultValue;
    }
}
