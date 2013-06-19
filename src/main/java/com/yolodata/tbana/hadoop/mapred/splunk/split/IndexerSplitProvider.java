package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.splunk.Service;
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
        numberOfSplits = getNumberOfSplits(conf, numberOfSplits);

        InputSplit[] splits = new InputSplit[numberOfSplits];

        JobRecordReader rr = new JobRecordReader(conf);

        List<Indexer> indexers = IndexerProvider.getIndexers(SplunkService.connect(conf));

        for(int i=0;i<indexers.size();i++)
        {
            Indexer indexer = indexers.get(i);
            Service service = SplunkService.connect(conf, indexer.getHost(), indexer.getPort());

            /*
               TODO: Refactor out create job logic from RecordReader, it should
               TODO: it should be in a separate class, so that it can be accessed from here too.
            */
            splits[i] = new SplunkSplit();
        }

        return splits;
    }

}
