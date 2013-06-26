package com.yolodata.tbana;

import cascading.tuple.Fields;
import com.yolodata.tbana.cascading.splunk.SplunkSearch;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class TestConfigurations {

    public static final String query = "search * sourcetype=\"mock\" | head 5 | table _raw";
    public static final String earliest_time = "-12mon";
    public static final String latest_time = "now";

    public static Configuration getConfigurationWithSplunkConfigured() {
        Configuration conf = new Configuration();

        conf.set(SplunkConf.SPLUNK_USERNAME, "admin");
        conf.set(SplunkConf.SPLUNK_PASSWORD, "changeme");
        conf.set(SplunkConf.SPLUNK_HOST, "localhost");
        conf.set(SplunkConf.SPLUNK_PORT, "9050");
        conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, earliest_time);
        conf.set(SplunkConf.SPLUNK_LATEST_TIME, latest_time);
        conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, query);

        return conf;
    }

    public static Properties getSplunkLoginAsProperties() {
        Properties properties = new Properties();

        properties.put(SplunkConf.SPLUNK_USERNAME, "admin");
        properties.put(SplunkConf.SPLUNK_PASSWORD, "changeme");
        properties.put(SplunkConf.SPLUNK_HOST, "localhost");
        properties.put(SplunkConf.SPLUNK_PORT, "9050");

        return properties;
    }

    public static SplunkSearch getSplunkSearch() {
        return new SplunkSearch(query,earliest_time,latest_time);
    }
}
