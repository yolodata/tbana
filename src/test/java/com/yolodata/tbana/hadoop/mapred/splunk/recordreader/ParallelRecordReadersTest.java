package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.google.common.collect.Lists;
import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    public void testSingleRecordReader() throws InterruptedException {
        startTest(1);
    }

    @Test
    public void testMultipleRecordReadersInParallel() throws Exception {
        startTest(3);
    }

    private void startTest(int concurrentSplits) throws InterruptedException {
        List<SplunkSplit> splits = getSplunkSplits(concurrentSplits);

        List<RecordReaderThread> threads = Lists.newArrayList();

        for(SplunkSplit split : splits) {
            RecordReaderThread thread = new RecordReaderThread(split, configuration);
            threads.add(thread);
        }

        waitForAllThreadsToFinish(threads);

        Map<LongWritable, List<Text>> expected = getExpectedResults();


        for(RecordReaderThread thread : threads) {
            Map<LongWritable, List<Text>> actual = thread.getResults();

            assert(expected.size() == actual.size());

            for(LongWritable key : actual.keySet())
                assert(expected.get(key).get(0).equals(actual.get(key).get(1)));
        }

    }

    private Map<LongWritable, List<Text>> getExpectedResults() {
        Map expected = new HashMap<LongWritable,List<Text>>();
        addKVToMap(expected, 0, "_raw");
        addKVToMap(expected, 1, "#<DateTime 2012-12-31T23:59:59.000Z> count=0");
        addKVToMap(expected, 2, "#<DateTime 2012-12-31T23:59:58.000Z> count=1");
        addKVToMap(expected, 3, "#<DateTime 2012-12-31T23:59:57.000Z> count=2");
        addKVToMap(expected, 4, "#<DateTime 2012-12-31T23:59:56.000Z> count=3");
        addKVToMap(expected, 5, "#<DateTime 2012-12-31T23:59:55.000Z> count=4");

        return expected;
    }

    private void addKVToMap(Map expected, int key, String value) {
        ArrayListTextWritable texts = new ArrayListTextWritable();
        texts.add(new Text(value));
        expected.put(new LongWritable(key), texts);
    }

    private void waitForAllThreadsToFinish(List<RecordReaderThread> threads) throws InterruptedException {
        for(Thread t : threads)
            t.join();
    }

    private ArrayList<SplunkSplit> getSplunkSplits(int numberOfSplits) {
        ArrayList<SplunkSplit> splunkSplits = Lists.newArrayList();

        for(int i=0;i<numberOfSplits;i++) {
            SplunkJob job = SplunkJob.createSplunkJob(splunkService,configuration);
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
        this.results = new HashMap<LongWritable, List<Text>>();
        this.start();
    }

    public Map getResults() {
        return results;
    }

    @Override
    public void run() {
        try {
            SplunkRecordReader recordReader = new JobRecordReader(conf);
            recordReader.initialize(split);
            LongWritable key = recordReader.createKey();
            List<Text> values = recordReader.createValue();

            while(recordReader.next(key,values)){
                results.put(key,values);
                key = recordReader.createKey();
                values = recordReader.createValue();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}