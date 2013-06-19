package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.splunk.Job;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class JobSplitProvider extends SplitProvider {

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        numberOfSplits = getNumberOfSplits(conf,numberOfSplits);

        InputSplit[] splits = new InputSplit[numberOfSplits];

        JobRecordReader rr = new JobRecordReader(conf);
        Job job = rr.createJob();
        rr.waitForJobDone(job);
        long numberOfEvents = rr.getNumberOfResultsFromJob(job);

        try {
            int resultsPerSplit = (int)numberOfEvents/numberOfSplits;
            int overflow = (int)numberOfEvents%numberOfSplits;

            boolean skipHeader = false;
            for(int i=0; i<numberOfSplits; i++) {
                int start;
                int end;

                start = i * resultsPerSplit;
                if(i>0)
                    start += overflow;

                if(i==numberOfSplits-1)
                    end= (int) numberOfEvents;
                else
                    end = start + resultsPerSplit;

                if(i==0)
                    end+=overflow;

                // header is always included, therefore, always add +1 to account for that.
                end++;

                // Skip header for all splits except first
                skipHeader = i>0;

                splits[i] = new SplunkSplit(job.getSid(), start, end, skipHeader);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
