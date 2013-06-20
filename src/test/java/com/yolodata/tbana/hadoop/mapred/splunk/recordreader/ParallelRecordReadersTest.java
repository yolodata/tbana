package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.google.common.collect.Lists;
import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParallelRecordReadersTest {

    private Service splunkService;
    private JobConf configuration;

    @Before
    public void setUp() {
        configuration = getJobConf();
        splunkService = SplunkService.connect(configuration);
    }

    @Test
    public void testSingleRecordReader() {

    }

    @Test
    public void testMultipleRecordReadersInParallel() throws Exception {

    }

    private void startTest(int concurrentSplits) throws InterruptedException {
        List<SplunkSplit> splits = getSplunkSplits(concurrentSplits);

        List<RecordReaderThread> threads = Lists.newArrayList();

        for(SplunkSplit split : splits) {
            RecordReaderThread thread = new RecordReaderThread(split, configuration);
            threads.add(thread);
        }

        waitForAllThreadsToFinish(threads);

        // check all results


    }

    private void waitForAllThreadsToFinish(List<RecordReaderThread> threads) throws InterruptedException {
        for(Thread t : threads)
            t.join();
    }

    private ArrayList<SplunkSplit> getSplunkSplits(int numberOfSplits) {
        ArrayList<SplunkSplit> splunkSplits = Lists.newArrayList();

        for(int i=0;i<numberOfSplits;i++) {
            SplunkJob job = SplunkJob.createSplunkJob(splunkService,configuration);
            job.waitForCompletion(500);
            int start = 0;
            int end = job.getNumberOfResultsFromJob(configuration) + 1;

            splunkSplits.add(new SplunkSplit(job.getJob().getSid(), start, end));
        }
        return splunkSplits;
    }

    private JobConf getJobConf() {
        JobConf jobConf = new JobConf();

        jobConf.set(SplunkConf.SPLUNK_USERNAME, "admin");
        jobConf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        jobConf.set(SplunkConf.SPLUNK_HOST, "localhost");
        jobConf.set(SplunkConf.SPLUNK_PORT, "8089");
        jobConf.set(SplunkConf.SPLUNK_EARLIEST_TIME, "2012-12-30T23:59:55.000");
        jobConf.set(SplunkConf.SPLUNK_LATEST_TIME, "2013-01-31T23:59:59.000");
        jobConf.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search * sourcetype=\"moc3\" | table sourcetype,_raw");

        return jobConf;
    }
}

class RecordReaderThread extends Thread {

    private SplunkSplit split;
    private JobConf conf;
    private Map<LongWritable,List<Text>> results;

    public RecordReaderThread(SplunkSplit split, JobConf conf) {
        this.split = split;
        this.conf = conf;
        this.start();
    }

    public Map getResults() {
        return results;
    }

    @Override
    public void run() {
        try {
            SplunkRecordReader recordReader = new JobRecordReader(conf);
            LongWritable key = recordReader.createKey();
            List<Text> values = recordReader.createValue();

            while(recordReader.next(key,values))
                results.put(key,values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}