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

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class ExportSplitProvider extends SplitProvider {

    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        InputSplit[] splits = new InputSplit[numberOfSplits];

        try {
            // ExportRecordReader rr = new ExportRecordReader(conf);
            int resultsPerSplit = 1; //(rr.getNumberOfResults()+1)/numberOfSplits;

            for(int i=0; i<numberOfSplits; i++) {
                int start = i * resultsPerSplit;
                int end = start + resultsPerSplit;
                splits[i] = new SplunkSplit(start, end);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return splits;
    }
}
