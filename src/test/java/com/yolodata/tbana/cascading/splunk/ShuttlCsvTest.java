package com.yolodata.tbana.cascading.splunk;

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

public class ShuttlCsvTest extends CascadingTestCase {

    String testData = "src/test/resources/multilineCSV.csv";

    String outputPath = "build/testTMP";

    @Test
    public void testShuttlCsv() throws IOException {
    runCSVLine("csvtest", testData);
    }

    public void runCSVLine( String path, String inputData) throws IOException
    {
        Properties properties = new Properties();

        ShuttlCsv inputScheme = new ShuttlCsv();
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