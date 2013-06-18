package com.yolodata.tbana.hadoop.mapred.splunk;


import com.splunk.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class JobInputFormat implements InputFormat<LongWritable, List<Text>> {

    @Override
    public RecordReader<LongWritable, List<Text>> getRecordReader(InputSplit inputSplit,
                                                                  JobConf configuration, Reporter reporter) throws IOException {
        JobRecordReader jobRecordReader = new JobRecordReader(configuration);

        jobRecordReader.initialize(inputSplit);

        return jobRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws RuntimeException, IOException {

        if(conf.get(SplunkRecordReader.INPUTFORMAT_SPLITS) != null)
            numberOfSplits = Integer.parseInt(conf.get(SplunkRecordReader.INPUTFORMAT_SPLITS));

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
