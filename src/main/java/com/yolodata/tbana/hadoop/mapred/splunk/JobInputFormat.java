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

        // Solve problem with configuring number of inputSplits
        if(conf.get(SplunkRecordReader.INPUTFORMAT_SPLITS) != null)
            numberOfSplits = Integer.parseInt(conf.get(SplunkRecordReader.INPUTFORMAT_SPLITS));

        InputSplit[] splits = new InputSplit[numberOfSplits];

        JobRecordReader rr = new JobRecordReader(conf);

        Job job = rr.createJob();

        rr.waitForJobDone(job);

        long numberOfEvents = rr.getNumberOfResultsFromJob(job);

        try {
            int resultsPerSplit = (int)Math.ceil((numberOfEvents/numberOfSplits));

            for(int i=0; i<numberOfSplits; i++) {
                int start = i * resultsPerSplit;
                int end = start + resultsPerSplit;
                splits[i] = new SplunkSplit(job.getSid(), start, end);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
