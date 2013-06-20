package com.yolodata.tbana.hadoop.mapred.splunk;

public class IndexerInputFormatTest extends SplunkInputFormatBaseTest {
    @Override
    protected String getMethodToTest() {
        return SplunkInputFormat.Mode.Indexer.toString();
    }
}
