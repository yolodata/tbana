package com.yolodata.tbana.hadoop.mapred.splunk.indexer;

import com.google.common.collect.Lists;
import com.splunk.Service;

import java.util.List;

public class IndexerProvider {

    public static List<Indexer> getIndexers(Service splunkService) {
        return Lists.newArrayList();
    }
}