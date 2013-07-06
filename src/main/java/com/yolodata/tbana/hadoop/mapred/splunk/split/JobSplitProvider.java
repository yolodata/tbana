package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class JobSplitProvider extends SplitProvider {

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        numberOfSplits = getNumberOfSplits(conf,numberOfSplits);
        InputSplit[] splits = new InputSplit[numberOfSplits];

        Service service = SplunkService.connect(conf);
        SplunkJob splunkJob = SplunkJob.createSplunkJob(service,conf);

        long numberOfEvents = splunkJob.getNumberOfResultsFromJob(conf);

        try {
            int resultsPerSplit = (int)numberOfEvents/numberOfSplits;

            boolean skipHeader;
            for(int i=0; i<numberOfSplits; i++) {
                int start = i * resultsPerSplit;
                int end = start + resultsPerSplit;

                // Skip header for all splits except first
                skipHeader = i>0;

                // Header is always present, so we always need to read an extra row.
                end++;

                if(i==numberOfSplits-1) {
                    long eventsLeft = numberOfEvents-(numberOfSplits*resultsPerSplit);
                    end += eventsLeft;
                }

                splits[i] = new SplunkSplit(splunkJob.getJob().getSid(), start, end, skipHeader);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
