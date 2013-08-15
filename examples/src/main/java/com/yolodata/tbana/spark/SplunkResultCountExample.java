/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.spark;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.spark.RDD.SplunkRDD;
import org.apache.hadoop.mapred.JobConf;
import spark.api.java.JavaSparkContext;

public class SplunkResultCountExample {
    public static void main(String [] args) {
        JavaSparkContext sparkContext = new JavaSparkContext("local","example");

        run(sparkContext);
    }

    private static void run(JavaSparkContext sparkContext) {
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
