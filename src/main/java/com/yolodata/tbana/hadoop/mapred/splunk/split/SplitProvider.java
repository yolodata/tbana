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

package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;


public abstract class SplitProvider {

    public abstract InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException;

    public static SplitProvider getProvider(SplunkInputFormat.Mode inputFormatMode) {
        switch (inputFormatMode) {
            case Job:
                return new JobSplitProvider();
            case Export:
                return new ExportSplitProvider();
            case Indexer:
                return new IndexerSplitProvider();
            default:
                throw new RuntimeException("SplitProvider for Mode " + inputFormatMode.toString() + " is not specified in SplitProvider.getProvider(Mode)");
            case TwoLayerIndexer:
                break;
        }
        return null;
    }

    protected int getNumberOfSplits(JobConf conf, int defaultValue) {
        if(conf.get(SplunkInputFormat.INPUTFORMAT_SPLITS) != null)
            defaultValue = Integer.parseInt(conf.get(SplunkInputFormat.INPUTFORMAT_SPLITS));
        return defaultValue;
    }
}
