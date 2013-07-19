package com.yolodata.tbana.hadoop.mapred.splunk.split;

import com.yolodata.tbana.hadoop.mapred.splunk.indexer.Indexer;
import org.junit.Test;

public class IndexerSplitTest extends InputSplitTest {

    @Test
    public void testSerialization() throws Exception {
        IndexerSplit is = new IndexerSplit(new Indexer("localhost",4711),"abc",0,1,false);
        IndexerSplit empty = new IndexerSplit();

        super.testSerialization(is,empty);
    }
}
