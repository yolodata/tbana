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

package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.ExportRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.SplunkRecordReader;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplitProvider;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.List;

public class SplunkInputFormat implements InputFormat<LongWritable, ArrayListTextWritable> {

    public static final String INPUTFORMAT_SPLITS = "splunk.inputformat.splits";
    public static final String INPUTFORMAT_MODE = "splunk.inputformat.mode";

    public static enum Mode { Job, Export, Indexer, TwoLayerIndexer};
    private static final Mode DEFAULT_MODE = Mode.Job;

    @Override
    public RecordReader<LongWritable,ArrayListTextWritable> getRecordReader(InputSplit inputSplit, JobConf configuration, Reporter reporter) throws IOException {
        SplunkConf splunkConf = new SplunkConf(configuration);
        SplunkRecordReader splunkRecordReader = getRecordReaderFromConf(splunkConf);
        splunkRecordReader.initialize(inputSplit);

        return splunkRecordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int i) throws IOException {

        Mode mode = getMethodFromConf(conf);
        SplitProvider sp = SplitProvider.getProvider(mode);

        return sp.getSplits(conf,i);
    }

    private static Mode getMethodFromConf(Configuration conf) {
        String mode = conf.get(INPUTFORMAT_MODE,"Job");

        for(Mode m : Mode.values())
            if(m.toString().equalsIgnoreCase(mode))
                return m;

        return DEFAULT_MODE;
    }

    public static SplunkRecordReader getRecordReaderFromConf(SplunkConf conf) throws IOException {
        Mode mode = getMethodFromConf(conf);
        switch(mode) {
            case Job:
                return new JobRecordReader(conf);
            case Export:
                return new ExportRecordReader(conf);
            case Indexer:
                return new JobRecordReader(conf);
            default:
                throw new RuntimeException("Cannot get SplunkRecordReader in SplunkInputFormat using mode "+ mode.toString());
        }

    }
}
