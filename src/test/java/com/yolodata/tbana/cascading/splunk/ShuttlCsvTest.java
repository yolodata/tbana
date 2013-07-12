package com.yolodata.tbana.cascading.splunk;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.testutils.FileTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
import com.yolodata.tbana.util.search.ShuttlDirectoryTreeFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class ShuttlCsvTest extends CascadingTestCase {

    private FileSystem fs;

    @Override
    public void setUp() throws Exception {
        fs = FileSystem.getLocal(new Configuration());
        fs.delete(new Path(TestUtils.TEST_FILE_PATH), true);
    }

    @Test
    public void testShuttlDirectory() throws IOException, InstantiationException, IllegalAccessException {
        ShuttlDirectoryTreeFactory directoryTreeFactory = new ShuttlDirectoryTreeFactory();

        Path index = directoryTreeFactory.addIndex(directoryTreeFactory.getIndexerPaths().get(0),"Index1");
        Path bucket = directoryTreeFactory.addBucket(index,"db_1_0_idx");

        String[] header = {"header", "_raw"};

        String file1Content = "header,_raw\n" +
                "\"ImCESTvlhu\",\"LOjZxHYGZy\"\n" +
                "\"kqYkcFhbSB\",\"RRLjuHCHze\"\n" +
                "\"kmeEaOcKTx\",\"mvIPrMSOSS\"\n" +
                "\"lzzPLYFGFU\",\"sGHTPVsYlF\"\n" +
                "\"zpUxVlTaAq\",\"ysoUYuyZKO\"";
        String file2Content = "header,_raw\n" +
                "\"fkBkKfCkuT\",\"BRlSkqHmHe\"\n" +
                "\"dWDJViEuot\",\"LcdkTQBLmu\"\n" +
                "\"ovQoDFATdn\",\"YewByxPXqN\"\n" +
                "\"tKBxjsSZmV\",\"luuOivALWj\"\n" +
                "\"mssAbiUnub\",\"NeYnIlDMdW\"";

        Path file1 = HadoopFileTestUtils.createPath(bucket.toString(),"file1.csv");
        Path file2 = HadoopFileTestUtils.createPath(bucket.toString(),"file2.csv");
        HadoopFileTestUtils.createFileWithContent(fs,file1,file1Content);
        HadoopFileTestUtils.createFileWithContent(fs,file2,file2Content);

        Path outputPath = new Path(FileTestUtils.getRandomTestFilepath());
        runCascadingJob(directoryTreeFactory.getRoot(),outputPath);

        String expectedContent = "0\theader\t_raw\n" +
                "12\tImCESTvlhu\tLOjZxHYGZy\n" +
                "38\tkqYkcFhbSB\tRRLjuHCHze\n" +
                "64\tkmeEaOcKTx\tmvIPrMSOSS\n" +
                "90\tlzzPLYFGFU\tsGHTPVsYlF\n" +
                "116\tzpUxVlTaAq\tysoUYuyZKO\n" +
                "153\tfkBkKfCkuT\tBRlSkqHmHe\n" +
                "179\tdWDJViEuot\tLcdkTQBLmu\n" +
                "205\tovQoDFATdn\tYewByxPXqN\n" +
                "231\ttKBxjsSZmV\tluuOivALWj\n" +
                "257\tmssAbiUnub\tNeYnIlDMdW\n";

        String actualResults = HadoopFileTestUtils.readMapReduceOutputFile(fs,outputPath);

        assertEquals(expectedContent,actualResults);

        directoryTreeFactory.remove();
    }

    public Flow runCascadingJob( Path inputPath, Path outputPath) throws IOException
    {
        Properties properties = new Properties();

        ShuttlCsv inputScheme = new ShuttlCsv(new SplunkDataQuery());
        TextLine outputScheme = new TextLine();

        Hfs input = new Hfs(inputScheme,inputPath.toString());
        Hfs output = new Hfs(outputScheme,outputPath.toString(),SinkMode.REPLACE);

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        return flow;
    }
}