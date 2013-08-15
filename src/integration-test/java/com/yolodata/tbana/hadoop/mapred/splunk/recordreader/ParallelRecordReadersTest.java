/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.google.common.collect.Lists;
import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.testutils.TestConfigurations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParallelRecordReadersTest {

    private Service splunkService;
    private SplunkConf configuration;

    @Before
    public void setUp() {
        configuration = TestConfigurations.getConfigurationWithSplunkConfigured();
        splunkService = SplunkService.connect(configuration);
    }

    @Test
    public void testSingleJobRecordReader() throws InterruptedException, IOException {
        configuration.set(SplunkInputFormat.INPUTFORMAT_MODE, SplunkInputFormat.Mode.Job.toString());
        startTest(configuration,1);
    }

    @Test
    public void testSingleExportRecordReader() throws InterruptedException, IOException {
        configuration.set(SplunkInputFormat.INPUTFORMAT_MODE, SplunkInputFormat.Mode.Export.toString());
        startTest(configuration,1);
    }

    @Test
    public void testMultipleJobRecordReadersInParallel() throws Exception {
        configuration.set(SplunkInputFormat.INPUTFORMAT_MODE, SplunkInputFormat.Mode.Job.toString());
        startTest(configuration,3);
    }


    @Test
    public void testMultipleExportRecordReadersInParallel() throws Exception {
        SplunkConf conf = new SplunkConf(configuration);
        conf.set(SplunkInputFormat.INPUTFORMAT_MODE, SplunkInputFormat.Mode.Export.toString());
        startTest(conf,3);
    }

    private void startTest(SplunkConf conf, int concurrentSplits) throws InterruptedException, IOException {
        List<SplunkSplit> splits = getSplunkSplits(concurrentSplits);
        List<RecordReaderThread> threads = Lists.newArrayList();

        for(SplunkSplit split : splits) {
            SplunkRecordReader splunkRecordReader = SplunkInputFormat.getRecordReaderFromConf(conf);

            RecordReaderThread thread = new RecordReaderThread(splunkRecordReader,split);
            threads.add(thread);
        }

        waitForAllThreadsToFinish(threads);

        Map<LongWritable, List<Text>> expected = getExpectedResults();

        for(RecordReaderThread thread : threads) {
            Map<LongWritable, List<Text>> actual = thread.getResults();

            assert(expected.size() == actual.size());

            for(LongWritable key : actual.keySet())
                assert(expected.get(key).get(0).equals(actual.get(key).get(0)));
        }
    }

    private Map<LongWritable, List<Text>> getExpectedResults() {
        Map expected = new HashMap<LongWritable,List<Text>>();
        addKVToMap(expected, 0, "_raw");
        addKVToMap(expected, 1, "count=4");
        addKVToMap(expected, 2, "count=3");
        addKVToMap(expected, 3, "count=2");
        addKVToMap(expected, 4, "count=1");
        addKVToMap(expected, 5, "count=0");

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

    private class RecordReaderThread extends Thread {
        private SplunkSplit split;
        private Map<LongWritable,List<Text>> results;
        private SplunkRecordReader recordReader;

        public RecordReaderThread(SplunkRecordReader recordReader, SplunkSplit split) {
            this.results = new HashMap<LongWritable, List<Text>>();
            this.recordReader = recordReader;
            this.split = split;

            this.start();
        }

        public Map getResults() {
            return results;
        }

        @Override
        public void run() {
            try {
                recordReader.initialize(split);
                LongWritable key = recordReader.createKey();
                ArrayListTextWritable values = recordReader.createValue();

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
}

