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

import com.splunk.JobExportArgs;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.io.InputStreamReader;

public class ExportRecordReader extends SplunkRecordReader {

    public ExportRecordReader(SplunkConf configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException {

        initPositions((SplunkSplit)inputSplit);

        is = splunkService.export(configuration.get(SplunkConf.SPLUNK_SEARCH_QUERY), getExportArgs());
        in = new InputStreamReader(is);
    }

    protected void initPositions(SplunkSplit inputSplit) {
        super.initPositions(inputSplit);
    }


    public JobExportArgs getExportArgs() {
        JobExportArgs jobExportArgs = new JobExportArgs();
        jobExportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);

        jobExportArgs.setLatestTime(configuration.get(SplunkConf.SPLUNK_LATEST_TIME));
        jobExportArgs.setEarliestTime(configuration.get(SplunkConf.SPLUNK_EARLIEST_TIME));

        jobExportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);

        return jobExportArgs;
    }
}
