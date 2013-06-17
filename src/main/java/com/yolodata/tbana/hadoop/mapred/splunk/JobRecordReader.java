package com.yolodata.tbana.hadoop.mapred.splunk;

import com.splunk.Job;
import com.splunk.JobArgs;
import com.splunk.JobResultsArgs;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStreamReader;

public class JobRecordReader extends SplunkRecordReader {

    public JobRecordReader(JobConf configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException {
        SplunkSplit splunkSplit = (SplunkSplit) inputSplit;
        Job job = getJob(splunkSplit.getJobID());

        JobResultsArgs resultsArgs = new JobResultsArgs();
        resultsArgs.setOutputMode(JobResultsArgs.OutputMode.CSV);


        //resultsArgs.setFieldList(new String[] {"sourcetype","_raw"});
        int totalLinesToGet = (int) (endPosition-startPosition);
        resultsArgs.setOffset((int) startPosition);
        resultsArgs.setCount(totalLinesToGet);

        is = job.getResults(resultsArgs);
        in = new InputStreamReader(is);
    }

    public Job createJob() {
        Job j = splunkService.getJobs().create(configuration.get(SPLUNK_SEARCH_QUERY), getJobArgs());

        return j;
    }


    protected JobArgs getJobArgs() {
        JobArgs jobArgs = new JobArgs();

        jobArgs.setLatestTime(configuration.get(SPLUNK_LATEST_TIME));
        jobArgs.setEarliestTime(configuration.get(SPLUNK_EARLIEST_TIME));

        return jobArgs;
    }

    protected void waitForJobDone(Job job) {
        while(!job.isDone())
        {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Job getJob(String jobID) {
        return splunkService.getJobs().get(jobID);
    }

    public int getNumberOfResultsFromJob(Job job) {
        return job.getEventCount();
    }
}
