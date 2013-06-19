package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class ExportSplitProvider extends SplitProvider {

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        InputSplit[] splits = new InputSplit[numberOfSplits];

        try {
            // ExportRecordReader rr = new ExportRecordReader(conf);
            int resultsPerSplit = 1; //(rr.getNumberOfResults()+1)/numberOfSplits;

            for(int i=0; i<numberOfSplits; i++) {
                int start = i * resultsPerSplit;
                int end = start + resultsPerSplit;
                splits[i] = new SplunkSplit(start, end);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
