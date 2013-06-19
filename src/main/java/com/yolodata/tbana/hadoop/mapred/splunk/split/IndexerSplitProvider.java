package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class IndexerSplitProvider extends SplitProvider{
    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {
        throw new NotImplementedException("Not implemented");
    }
}
