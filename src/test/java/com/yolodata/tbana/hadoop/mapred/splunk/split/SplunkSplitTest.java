package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.junit.Test;

public class SplunkSplitTest extends IndexerSplitTest{

    @Test
    public void testSerialization() throws Exception {
        SplunkSplit split = new SplunkSplit("jobID",0,1,false);
        SplunkSplit empty = new SplunkSplit();

        super.testSerialization(split,empty);
    }
}
