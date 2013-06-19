package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.splunk.JobExportArgs;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
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

        is = splunkService.export(configuration.get(SPLUNK_SEARCH_QUERY), getExportArgs());
        in = new InputStreamReader(is);
    }

    protected void initPositions(SplunkSplit inputSplit) {
        super.initPositions(inputSplit);
    }


    public JobExportArgs getExportArgs() {
        JobExportArgs jobExportArgs = new JobExportArgs();
        jobExportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);
        // jobExportArgs.add("offset", startPosition); does not work in SplunkAPI
        //long totalLinesToGet = endPosition-startPosition;
        //jobExportArgs.add("count",totalLinesToGet);

        jobExportArgs.setLatestTime(configuration.get(SPLUNK_LATEST_TIME));
        jobExportArgs.setEarliestTime(configuration.get(SPLUNK_EARLIEST_TIME));

        jobExportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);

        return jobExportArgs;
    }
}
