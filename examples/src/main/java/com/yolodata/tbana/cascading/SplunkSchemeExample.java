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

package com.yolodata.tbana.cascading;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkDataQuery;
import com.yolodata.tbana.cascading.splunk.SplunkScheme;
import com.yolodata.tbana.cascading.splunk.SplunkTap;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;

import java.util.Properties;

public class SplunkSchemeExample {

    private static final String PATH_TO_OUTPUT = "examples/output/splunk-scheme-example";

    public static void main(String [] args) {

        Properties properties = new Properties();

        properties.put(SplunkConf.SPLUNK_USERNAME, "admin");
        properties.put(SplunkConf.SPLUNK_PASSWORD, "changeIt");
        properties.put(SplunkConf.SPLUNK_HOST, "localhost");
        properties.put(SplunkConf.SPLUNK_PORT, "9050");

        SplunkDataQuery splunkSearch = new SplunkDataQuery();
        SplunkScheme inputScheme = new SplunkScheme(splunkSearch);
        SplunkTap input = new SplunkTap(properties,inputScheme);

        TextLine outputScheme = new TextLine();
        Hfs output = new Hfs( outputScheme, PATH_TO_OUTPUT, SinkMode.REPLACE );

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector().connect( input, output, pipe );

        flow.complete();
    }
}
