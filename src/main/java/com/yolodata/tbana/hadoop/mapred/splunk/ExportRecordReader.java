package com.yolodata.tbana.hadoop.mapred.splunk;


import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStreamReader;

public class ExportRecordReader extends SplunkRecordReader {

    public ExportRecordReader(JobConf configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException {

        initPositions((SplunkSplit)inputSplit);

        is = splunkService.export(configuration.get(SPLUNK_SEARCH_QUERY), getJobExportArgs());
        in = new InputStreamReader(is);
    }

    protected void initPositions(SplunkSplit inputSplit) {
        super.initPositions(inputSplit);
    }

}
