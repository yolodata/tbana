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

        Tuple expected = new Tuple(new LongWritable(0),new Text("sourcetype"),new Text("_raw"));
        Tuple actual = iterator.next().getTuple();
        assertEquals(expected, actual);

        expected.clear();
        expected.addAll(new LongWritable(1), new Text("moc3"), new Text("#<DateTime 2012-12-31T23:59:59.000Z> count=0"));
        actual = iterator.next().getTuple();
        assertEquals(expected, actual);

        expected.clear();
        expected.addAll(new LongWritable(2), new Text("moc3"), new Text("#<DateTime 2012-12-31T23:59:58.000Z> count=1"));
        actual = iterator.next().getTuple();
        assertEquals(expected, actual);
    }
}
