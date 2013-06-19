package com.yolodata.tbana.hadoop.mapred.splunk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class SplunkConf {
    public static final String SPLUNK_USERNAME = "splunk.username";
    public static final String SPLUNK_PASSWORD = "splunk.password";
    public static final String SPLUNK_HOST = "splunk.host";
    public static final String SPLUNK_PORT = "splunk.port";
    public static final String SPLUNK_SEARCH_QUERY = "splunk.search.query";
    public static final String SPLUNK_EARLIEST_TIME = "splunk.search.earliest_time";
    public static final String SPLUNK_LATEST_TIME = "splunk.search.latest_time";
    public static final String SPLUNK_FIELD_LIST = "splunk.search.field_list";

    public static void validateConfiguration(JobConf configuration) throws SplunkConfigurationException {
        validateLoginConfiguration(configuration);
        validateSearchConfiguration(configuration);
    }

    public static void validateLoginConfiguration(Configuration configuration) {
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_USERNAME);
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_PASSWORD);
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_HOST);
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_PORT);
    }

    public static void validateSearchConfiguration(Configuration configuration) {
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_SEARCH_QUERY);
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_EARLIEST_TIME);
        requireKeyInConfiguration(configuration,SplunkConf.SPLUNK_LATEST_TIME);
    }

    public static boolean keyExists(Configuration configuration, String key) {
        return configuration.get(key) != null;
    }

    public static void requireKeyInConfiguration(Configuration configuration, String key) {
        if(!keyExists(configuration,key))
            throw new SplunkConfigurationException("Key "+key+" is required in JobConf configuration and is currently not set\n");
    }
}

class SplunkConfigurationException extends RuntimeException {
    public SplunkConfigurationException(String message) {
        super(message);
    }
}