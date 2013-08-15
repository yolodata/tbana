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
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.cascading.shuttl.ShuttlCsv;

import java.util.Properties;

public class ShuttlCsvExample {

    private static final String PATH_TO_SHUTTL_ARCHIVE = "examples/resources/shuttl";
    private static final String PATH_TO_OUTPUT = "examples/output/shuttl-example";

    public static void main(String [] args) {

        ShuttlCsv csv = new ShuttlCsv();
        Hfs input = new Hfs(csv,PATH_TO_SHUTTL_ARCHIVE);

        TextLine outputScheme = new TextLine();
        Hfs output = new Hfs(outputScheme, PATH_TO_OUTPUT);
        Pipe pipe = new Pipe( "test" );

        Properties properties = new Properties();
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();
    }
}
