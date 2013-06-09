package com.yolodata.tbana.cascading.csv;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
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
        Object[][] results = new Object[][]{
                {"header1","header2"},
                {"Column1", "Column 2 using one rows"},
                {"c1", "c2"}
        };

        Tuple[] tuples = new Tuple[results.length];

        for( int i = 0; i < results.length; i++ )
            tuples[ i ] = new Tuple( results[ i ] );

        Properties properties = new Properties();


        CSVLine inputScheme = new CSVLine();
        TextDelimited outputScheme = new TextDelimited();

        Hfs input = new Hfs( inputScheme, inputData );
        Hfs output = new Hfs( outputScheme, outputPath + "/quoted/" + path, SinkMode.REPLACE );

        Pipe pipe = new Pipe( "testCSVLinePipe" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        validateLength( flow, results.length, 2 );


        TupleEntryIterator iterator = flow.openSource();

        int count = 0;
        while( iterator.hasNext() )
        {
            Tuple tuple = iterator.next().getTuple();
            assertEquals( tuples[ count++ ], tuple );
        }

        iterator = flow.openSink();

        count = 0;
        while( iterator.hasNext() )
        {
            Tuple tuple = iterator.next().getTuple();
            assertEquals( tuples[ count++ ], tuple );
        }
    }

    @Test
    public void testHeader() throws IOException
    {
        Properties properties = new Properties();

        Class[] types = new Class[]{String.class, String.class};
        Fields fields = new Fields( "first", "second");

        Hfs input = new Hfs( new TextDelimited( fields, true, ",", "\"", types ), testData );
        Hfs output = new Hfs( new TextDelimited( fields, ",", "\"", types ), outputPath + "/header", SinkMode.REPLACE );

        Pipe pipe = new Pipe( "pipe" );

        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        validateLength( flow, 2, 2 );
    }
}