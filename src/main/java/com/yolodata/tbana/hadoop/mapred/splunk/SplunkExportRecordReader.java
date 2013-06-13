package com.yolodata.tbana.hadoop.mapred.splunk;


import com.yolodata.tbana.hadoop.mapred.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.CSVReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import com.splunk.*;

public class SplunkExportRecordReader implements RecordReader<LongWritable, List<Text>> {

    private final JobConf configuration;
    private long currentPosition;
    private long startPosition;
    private long endPosition;

    private InputStream is;
    private Reader in;

    public static final String SPLUNK_USERNAME = "splunk.username";
    public static final String SPLUNK_PASSWORD = "splunk.password";
    public static final String SPLUNK_HOST = "splunk.host";
    public static final String SPLUNK_PORT = "splunk.port";
    public static final String SPLUNK_SEARCH_QUERY = "splunk.search.query";


    private Service splunkService;
    private CSVReader reader;

    public SplunkExportRecordReader(JobConf configuration) throws IOException {

        this.configuration = configuration;
        validateConfiguration(this.configuration);
    }

    private void validateConfiguration(JobConf configuration) throws SplunkConfigurationException {
        if(configuration.get(SPLUNK_USERNAME) == null ||
                configuration.get(SPLUNK_PASSWORD) == null ||
                configuration.get(SPLUNK_HOST) == null ||
                configuration.get(SPLUNK_PORT) == null ||
                configuration.get(SPLUNK_SEARCH_QUERY) == null)
            throw new SplunkConfigurationException("Missing one or more of the following required configurations in JobConf:\n" +
                    SPLUNK_USERNAME + "\n" +
                    SPLUNK_PASSWORD + "\n" +
                    SPLUNK_HOST + "\n" +
                    SPLUNK_PORT + "\n" +
                    SPLUNK_SEARCH_QUERY + "\n");

    }

    public int getNumberOfResults() {
        Job searchJob = splunkService.search(configuration.get(SPLUNK_SEARCH_QUERY), getJobExportArgs()).finish();

        return searchJob.getEventCount();
    }

    public void initialize(InputSplit inputSplit) throws IOException {

        initPositions((SplunkSplit) inputSplit);

        ServiceArgs serviceArgs = getLoginArgs();
        splunkService = Service.connect(serviceArgs);

        is = splunkService.export(configuration.get(SPLUNK_SEARCH_QUERY), getJobExportArgs());

    }

    private void initPositions(SplunkSplit inputSplit) {
        SplunkSplit split = (SplunkSplit) inputSplit;
        startPosition = split.getStart();
        endPosition = split.getEnd();
        currentPosition = startPosition;
    }

    private JobExportArgs getJobExportArgs() {
        JobExportArgs jobExportArgs = new JobExportArgs();
        jobExportArgs.setOutputMode(JobExportArgs.OutputMode.JSON);
        jobExportArgs.add("offset", startPosition);
        jobExportArgs.setLatestTime("-12h");
        jobExportArgs.setEarliestTime("now"); // Add to JobConf.

        jobExportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
        long totalLinesToGet = endPosition-startPosition;
        jobExportArgs.add("count",totalLinesToGet);

        return jobExportArgs;
    }

    private ServiceArgs getLoginArgs() {
        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername(configuration.get(SPLUNK_USERNAME));
        loginArgs.setPassword(configuration.get(SPLUNK_PASSWORD));
        loginArgs.setHost(configuration.get(SPLUNK_HOST));
        loginArgs.setPort(configuration.getInt(SPLUNK_PORT, 8080));

        return loginArgs;

    }

    @Override
    public boolean next(LongWritable key, List<Text> value) throws IOException {
        //TODO: Extract CSV-logic from CSVRecordReader to separate file and use here to read lines from InputStream

        if(currentPosition == endPosition)
            return false;

        InputStreamReader streamReader = new InputStreamReader(is);
        reader = new CSVReader(streamReader);

        if(key == null) key = createKey();
        if(value == null) value = createValue();

        int bytesRead = reader.readLine(value);
        if(bytesRead == 0) {
            key = null;
            value = null;

            return false;
        }

        key.set(currentPosition);
        currentPosition++;
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public List<Text> createValue() {
        return new ArrayListTextWritable();
    }

    @Override
    public long getPos() throws IOException {
        return currentPosition;
    }

    @Override
    public void close() throws IOException {
        try {
            // Close splunk connection/splunk export

        } catch(Exception e){
            throw new IOException(e);
        }

    }

    @Override
    public float getProgress() throws IOException {
        if (startPosition == endPosition) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPosition - startPosition) / (float) (endPosition - startPosition));
        }
    }

    public void setCurrentPosition(long currentPosition) {
        this.currentPosition = currentPosition;
    }

    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }

    public void setEndPosition(long endPosition) {
        this.endPosition = endPosition;
    }

    private class SplunkConfigurationException extends RuntimeException {
        public SplunkConfigurationException(String message) {
            super(message);
        }
    }
}
