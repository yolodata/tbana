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

package com.yolodata.tbana.cascading.csv;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;
import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;


public class CSVLineTest extends CascadingTestCase
{
    String testData = "src/test/resources/multilineCSV.csv";

    String outputPath = "build/testTMP";

    @Test
    public void testCSVLine() throws IOException {
        runCSVLine("csvtest", testData);
    }

    public void runCSVLine( String path, String inputData) throws IOException
    {
        Properties properties = new Properties();

        CSVLine inputScheme = new CSVLine();
        TextLine outputScheme = new TextLine();

        Hfs input = new Hfs( inputScheme, inputData );
        Hfs output = new Hfs( outputScheme, outputPath + "/quoted/" + path, SinkMode.REPLACE );

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        validateLength( flow, 4, 2 ); // The file contains 4 rows, however there are only 3 CSV rows (inc the header row)


        TupleEntryIterator iterator = flow.openSource();

        ArrayListTextWritable expected = new ArrayListTextWritable();

        expected.add(new Text("header1"));
        expected.add(new Text("header2"));
        assertEquals(expected, iterator.next().getTuple().getObject(1));

        expected.clear();
        expected.add(new Text("Column1"));
        expected.add(new Text("Column 2 using\ntwo rows"));
        assertEquals(expected, iterator.next().getTuple().getObject(1));

        expected.clear();
        expected.add(new Text("c1"));
        expected.add(new Text("c2"));
        assertEquals(expected, iterator.next().getTuple().getObject(1));
    }
}