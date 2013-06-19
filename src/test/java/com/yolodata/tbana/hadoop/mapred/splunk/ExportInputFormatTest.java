package com.yolodata.tbana.hadoop.mapred.splunk;

public class ExportInputFormatTest extends SplunkInputFormatBaseTest {

    @Override
    protected String getMethodToTest() {
        return SplunkInputFormat.Method.Export.toString();
    }
}
