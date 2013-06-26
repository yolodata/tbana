package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.splunk.Service;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.indexer.Indexer;
import com.yolodata.tbana.hadoop.mapred.splunk.indexer.IndexerProvider;
import com.yolodata.tbana.hadoop.mapred.splunk.recordreader.JobRecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

public class IndexerSplitProvider extends SplitProvider{
    @Override
    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {

        Service mainService = SplunkService.connect(conf);
        List<Indexer> indexers = IndexerProvider.getIndexers(SplunkService.connect(conf));

        Indexer mainIndexer = new Indexer(mainService.getHost(), mainService.getPort());
        indexers.add(0, mainIndexer);

        InputSplit[] splits = new InputSplit[indexers.size()];

        for(int i=0;i<indexers.size();i++)
        {
            Indexer indexer = indexers.get(i);
            Service service = SplunkService.connect(conf, indexer.getHost(), indexer.getPort());

            SplunkJob splunkJob = SplunkJob.createSplunkJob(service,conf);

            int start = 0;

            // Add one for the header
            int end = splunkJob.getNumberOfResultsFromJob(conf) + 1;
            boolean skipHeader = i>0;

            splits[i] = new IndexerSplit(indexer,splunkJob.getJob().getSid(), start, end, skipHeader);
        }

        return splits;
    }

}
