package com.yolodata.tbana.hadoop.mapred.splunk.indexer;

import com.google.common.collect.Lists;
import com.splunk.DistributedPeer;
import com.splunk.Service;

import java.util.Collection;
import java.util.List;

public class IndexerProvider {

    public static List<Indexer> getIndexers(Service splunkService) {
        List<Indexer> indexers = Lists.newArrayList();
        Collection<DistributedPeer> distributedPeers = splunkService.getDistributedPeers().values();
        for(DistributedPeer peer : distributedPeers) {
            indexers.add(new Indexer(peer.getService().getHost(),peer.getService().getPort()));
        }

        return indexers;
    }
}