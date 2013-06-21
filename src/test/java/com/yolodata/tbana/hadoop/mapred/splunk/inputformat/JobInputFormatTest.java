package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;

public class JobInputFormatTest extends SplunkInputFormatBaseTest {

    @Override
    protected String getMethodToTest() {
        return SplunkInputFormat.Mode.Job.toString();
    }
}
