package com.yolodata.tbana;

import com.yolodata.tbana.cascading.splunk.SplunkSearch;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class TestConfigurations {

    public static final String query = "search * sourcetype=\"moc3\" | table sourcetype,_raw";
    public static final String earliest_time = "2012-12-30T23:59:55.000";
    public static final String latest_time = "2013-01-31T23:59:59.000";

    public static Configuration getConfigurationWithSplunkConfigured() {
        Configuration conf = new Configuration();

        conf.set(SplunkConf.SPLUNK_USERNAME, "admin");
        conf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        conf.set(SplunkConf.SPLUNK_HOST, "localhost");
        conf.set(SplunkConf.SPLUNK_PORT, "8089");
        conf.set(SplunkConf.SPLUNK_EARLIEST_TIME, earliest_time);
        conf.set(SplunkConf.SPLUNK_LATEST_TIME, latest_time);
        conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, query);

        return conf;
    }

    public static Properties getSplunkLoginAsProperties() {
        Properties properties = new Properties();

        properties.put(SplunkConf.SPLUNK_USERNAME, "admin");
        properties.put(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        properties.put(SplunkConf.SPLUNK_HOST, "localhost");
        properties.put(SplunkConf.SPLUNK_PORT, "8089");

        return properties;
    }

    public static SplunkSearch getSplunkSearch() {
        return new SplunkSearch(query,earliest_time,latest_time);
    }
}
