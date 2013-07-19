package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;

public class IndexerInputFormatTest extends SplunkInputFormatBaseTest {
    @Override
    protected String getMethodToTest() {
        return SplunkInputFormat.Mode.Indexer.toString();
    }
}
