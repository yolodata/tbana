package com.yolodata.tbana.cascading.splunk;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import com.yolodata.tbana.TestConfigurations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class SplunkSchemeTest extends CascadingTestCase {

    String testData = "src/test/resources/multilineCSV.csv";
    String outputPath = "build/testTMP";

    Configuration conf;

    @Before
    public void setUp() {
        conf = TestConfigurations.getConfigurationWithSplunkConfigured();
    }

    @Test
    public void testSplunkScheme() throws IOException {

        runSplunkScheme("csvtest", testData);

    }

    public void runSplunkScheme(String path, String inputData) throws IOException
    {
        Properties properties = TestConfigurations.getSplunkLoginAsProperties();

        SplunkScheme inputScheme = new SplunkScheme(TestConfigurations.getSplunkSearch());
        TextLine outputScheme = new TextLine();

        SplunkTap input = new SplunkTap(inputScheme);
        Hfs output = new Hfs( outputScheme, outputPath + "/quoted/" + path, SinkMode.REPLACE );

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        validateLength( flow, 6, 2 );


        TupleEntryIterator iterator = flow.openSource();
        String [] expectedRows = new String[] {
                "0,sourcetype,_raw",
                "1,moc3,#<DateTime 2012-12-31T23:59:59.000Z> count=0",
                "2,moc3,#<DateTime 2012-12-31T23:59:58.000Z> count=1",
                "3,moc3,#<DateTime 2012-12-31T23:59:57.000Z> count=2",
                "4,moc3,#<DateTime 2012-12-31T23:59:56.000Z> count=3",
                "5,moc3,#<DateTime 2012-12-31T23:59:55.000Z> count=4"
        };
        for(String expectedRow : expectedRows)
            checkResults(iterator.next().getTuple(), expectedRow);
    }

    private void checkResults(Tuple actual, String row) {
        String [] rowValues = row.split(",");
        Tuple expected = new Tuple(new LongWritable(Long.parseLong(rowValues[0])),
                new Text(rowValues[1]),
                new Text(rowValues[2]));
        assertEquals(expected, actual);
    }
}
