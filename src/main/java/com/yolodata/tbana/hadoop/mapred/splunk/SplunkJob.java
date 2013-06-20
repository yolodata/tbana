package com.yolodata.tbana.hadoop.mapred.splunk;

import com.splunk.Job;
import com.splunk.JobArgs;
import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import org.apache.hadoop.conf.Configuration;

public class SplunkJob {

    private Job job;

    public SplunkJob(Job job) {
        this.job = job;
    }

    public static SplunkJob getSplunkJob(Service service, String jobId) {
        Job job = service.getJobs().get(jobId);
        return new SplunkJob(job);
    }

    public static SplunkJob createSplunkJob(Service service, Configuration conf) {
        SplunkConf.validateSearchConfiguration(conf);

        String searchQuery = conf.get(SplunkConf.SPLUNK_SEARCH_QUERY);
        JobArgs jobArgs = getJobArgs(conf);
        Job job = service.getJobs().create(searchQuery, jobArgs);
        return new SplunkJob(job);
    }

    public void waitForCompletion(int refreshRate) {
        while (!job.isDone()) {
            try {
                Thread.sleep(refreshRate);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static JobArgs getJobArgs(Configuration configuration) {
        JobArgs jobArgs = new JobArgs();

        jobArgs.setLatestTime(configuration.get(SplunkConf.SPLUNK_LATEST_TIME));
        jobArgs.setEarliestTime(configuration.get(SplunkConf.SPLUNK_EARLIEST_TIME));

        return jobArgs;
    }

    public Job getJob() {
        return job;
    }

    public int getNumberOfResultsFromJob() {
        return job.getEventCount();
    }
}