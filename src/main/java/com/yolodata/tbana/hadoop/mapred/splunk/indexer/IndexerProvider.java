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
            String[] ipAndPort= peer.getName().split(":");
            indexers.add(new Indexer(ipAndPort[0], Integer.parseInt(ipAndPort[1])));
        }

        return indexers;
    }
}