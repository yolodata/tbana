package com.yolodata.tbana.hadoop.mapred.splunk.recordreader;

import com.splunk.JobResultsArgs;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkJob;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkService;
import com.yolodata.tbana.hadoop.mapred.splunk.split.IndexerSplit;
import com.yolodata.tbana.hadoop.mapred.splunk.split.SplunkSplit;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class JobRecordReader extends SplunkRecordReader {

    private boolean skipHeader;

    private SplunkJob splunkJob;

    public JobRecordReader(Configuration configuration) throws IOException {
        super(configuration);
    }

    @Override
    public void initialize(InputSplit inputSplit) throws IOException {
        SplunkSplit splunkSplit = (SplunkSplit) inputSplit;
        super.initPositions(splunkSplit);

        // TODO: This should be refactored to SplunkInputFormat.getRecordReader()
        if(splunkSplit instanceof IndexerSplit) {
            IndexerSplit split = (IndexerSplit) splunkSplit;
            splunkService = SplunkService.connect(configuration,split.getIndexer().getHost(), split.getIndexer().getPort());
        }

        skipHeader = splunkSplit.getSkipHeader();

        splunkJob = SplunkJob.getSplunkJob(splunkService,splunkSplit.getJobID());

        splunkJob.waitForCompletion(1000);

        JobResultsArgs resultsArgs = new JobResultsArgs();
        resultsArgs.setOutputMode(JobResultsArgs.OutputMode.CSV);

        setFieldList(resultsArgs);

        int totalLinesToGet = (int) (endPosition-startPosition);
        resultsArgs.setOffset((int) startPosition);
        resultsArgs.setCount(totalLinesToGet);

        is = splunkJob.getJob().getResults(resultsArgs);
        in = new InputStreamReader(is);
    }

    @Override
    public boolean next(LongWritable key, ArrayListTextWritable value) throws IOException {

        if(currentPosition == endPosition)
            return false;

        if(currentPosition == startPosition && skipHeader)
            super.next(key,value); //skip header

        return super.next(key,value);
    }

    private void setFieldList(JobResultsArgs resultsArgs) {
        String fields = configuration.get(SplunkConf.SPLUNK_FIELD_LIST);
        if(fields == null)
            return;

        resultsArgs.setFieldList(fields.split(","));
    }


}
