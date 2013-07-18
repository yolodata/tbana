package com.yolodata.tbana.spark;

import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.spark.RDD.SplunkRDD;
import org.apache.hadoop.mapred.JobConf;
import spark.api.java.JavaSparkContext;

public class SplunkResultCountExample {
    public static void main(String [] args) {
        JavaSparkContext sparkContext = new JavaSparkContext("local","example");

        JobConf conf = new JobConf();

        conf.set(SplunkConf.SPLUNK_USERNAME, "admin");
        conf.set(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        conf.set(SplunkConf.SPLUNK_HOST, "localhost");
        conf.set(SplunkConf.SPLUNK_PORT, "9050");

        SplunkDataQuery query = new SplunkDataQuery();
        conf.set(SplunkConf.SPLUNK_EARLIEST_TIME,query.getEarliestTimeString());
        conf.set(SplunkConf.SPLUNK_LATEST_TIME, query.getLatestTimeString());
        conf.set(SplunkConf.SPLUNK_SEARCH_QUERY, query.getSplunkQuery());

        SplunkRDD rdd = new SplunkRDD(sparkContext.sc(),conf,2);

        System.out.println("Line count: " + rdd.count());
    }
}
