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

import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.indexer.Indexer;
import com.yolodata.tbana.hadoop.mapred.splunk.indexer.IndexerProvider;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

public class IndexerSplitProvider extends SplitProvider{
    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        SplunkConf splunkConf = new SplunkConf(conf);
        Service mainService = SplunkService.connect(splunkConf);
        List<Indexer> indexers = IndexerProvider.getIndexers(SplunkService.connect(splunkConf));

        Indexer mainIndexer = new Indexer(mainService.getHost(), mainService.getPort());
        indexers.add(0, mainIndexer);

        InputSplit[] splits = new InputSplit[indexers.size()];

        for(int i=0;i<indexers.size();i++)
        {
            Indexer indexer = indexers.get(i);
            Service service = SplunkService.connect(splunkConf, indexer.getHost(), indexer.getPort());

            SplunkJob splunkJob = SplunkJob.createSplunkJob(service,splunkConf);

            int start = 0;

            // Add one for the header
            int end = splunkJob.getNumberOfResultsFromJob(splunkConf) + 1;
            boolean skipHeader = i>0;

            splits[i] = new IndexerSplit(indexer,splunkJob.getJob().getSid(), start, end, skipHeader);
        }

        return splits;
    }

}
