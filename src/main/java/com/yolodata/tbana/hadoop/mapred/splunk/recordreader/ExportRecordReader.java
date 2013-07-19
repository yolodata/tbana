package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.splunk.JobExportArgs;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.io.InputStreamReader;

public class ExportRecordReader extends SplunkRecordReader {

    public ExportRecordReader(SplunkConf configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException {

        initPositions((SplunkSplit)inputSplit);

        is = splunkService.export(configuration.get(SplunkConf.SPLUNK_SEARCH_QUERY), getExportArgs());
        in = new InputStreamReader(is);
    }

    protected void initPositions(SplunkSplit inputSplit) {
        super.initPositions(inputSplit);
    }


    public JobExportArgs getExportArgs() {
        JobExportArgs jobExportArgs = new JobExportArgs();
        jobExportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);

        jobExportArgs.setLatestTime(configuration.get(SplunkConf.SPLUNK_LATEST_TIME));
        jobExportArgs.setEarliestTime(configuration.get(SplunkConf.SPLUNK_EARLIEST_TIME));

        jobExportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);

        return jobExportArgs;
    }
}
