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
import java.util.List;

import com.splunk.*;

public class SplunkExportRecordReader implements RecordReader<LongWritable, List<Text>> {

    private final JobConf configuration;
    private long currentPosition;
    private long startPosition;
    private long endPosition;

    private Service splunkService;
    private InputStream is;
    private InputStreamReader in;
    private CSVReader reader;


    public static final String SPLUNK_USERNAME = "splunk.username";
    public static final String SPLUNK_PASSWORD = "splunk.password";
    public static final String SPLUNK_HOST = "splunk.host";
    public static final String SPLUNK_PORT = "splunk.port";
    public static final String SPLUNK_SEARCH_QUERY = "splunk.search.query";
    public static final String SPLUNK_EARLIEST_TIME = "splunk.search.earliest_time";
    public static final String SPLUNK_LATEST_TIME = "splunk.search.latest_time";



    public SplunkExportRecordReader(JobConf configuration) throws IOException {

        this.configuration = configuration;
        validateConfiguration(this.configuration);
        setupService();
    }

    private void validateConfiguration(JobConf configuration) throws SplunkConfigurationException {
        if(configuration.get(SPLUNK_USERNAME) == null ||
                configuration.get(SPLUNK_PASSWORD) == null ||
                configuration.get(SPLUNK_HOST) == null ||
                configuration.get(SPLUNK_PORT) == null ||
                configuration.get(SPLUNK_SEARCH_QUERY) == null ||
                configuration.get(SPLUNK_EARLIEST_TIME) == null ||
                configuration.get(SPLUNK_LATEST_TIME) == null)
            throw new SplunkConfigurationException("Missing one or more of the following required configurations in JobConf:\n" +
                    SPLUNK_USERNAME + "\n" +
                    SPLUNK_PASSWORD + "\n" +
                    SPLUNK_HOST + "\n" +
                    SPLUNK_PORT + "\n" +
                    SPLUNK_SEARCH_QUERY + "\n" +
                    SPLUNK_EARLIEST_TIME + "\n" +
                    SPLUNK_LATEST_TIME + "\n");

    }

    public int getNumberOfResults() throws InterruptedException {
        Job searchJob = splunkService.search(configuration.get(SPLUNK_SEARCH_QUERY), getJobExportArgs());
        while (!searchJob.isDone()){
            Thread.sleep(1000);
        }
        return searchJob.getEventCount();
    }

    public void initialize(InputSplit inputSplit) throws IOException {

        initPositions((SplunkSplit) inputSplit);


        is = splunkService.export(configuration.get(SPLUNK_SEARCH_QUERY), getJobExportArgs());
        in = new InputStreamReader(is);
    }

    private void setupService() {
        ServiceArgs serviceArgs = getLoginArgs();
        splunkService = Service.connect(serviceArgs);
    }

    private void initPositions(SplunkSplit inputSplit) {
        startPosition = inputSplit.getStart();
        endPosition = inputSplit.getEnd();
        currentPosition = startPosition;
    }

    private JobExportArgs getJobExportArgs() {
        JobExportArgs jobExportArgs = new JobExportArgs();
        jobExportArgs.setOutputMode(JobExportArgs.OutputMode.CSV);
        jobExportArgs.add("offset", startPosition);
        jobExportArgs.setLatestTime(configuration.get(SPLUNK_LATEST_TIME));
        jobExportArgs.setEarliestTime(configuration.get(SPLUNK_EARLIEST_TIME));

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
        if(currentPosition == endPosition)
            return false;

        reader = new CSVReader(in);

        if(key == null) key = createKey();
        if(value == null) value = createValue();

        int bytesRead = reader.readLine(value);

        if(bytesRead == 0) {
            key = null;
            value = null;
            return false;
        }

        key.set(currentPosition++);
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
            if(is!=null) {
                is.close();
                is=null;
            }

            if(in!=null) {
                in.close();
                in=null;
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

    private class SplunkConfigurationException extends RuntimeException {
        public SplunkConfigurationException(String message) {
            super(message);
        }
    }
}
