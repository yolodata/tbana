package com.yolodata.tbana.hadoop.mapred.splunk;

public class JobInputFormatTest extends SplunkInputFormatBaseTest {

    @Override
    protected String getMethodToTest() {
        return SplunkInputFormat.Method.Job.toString();
    }
}
