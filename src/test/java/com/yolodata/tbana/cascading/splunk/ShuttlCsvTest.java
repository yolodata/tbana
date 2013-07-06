package com.yolodata.tbana.cascading.splunk;

import cascading.CascadingTestCase;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.testutils.FileTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
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
        Path directory = FileSystemTestUtils.createEmptyDir(fs);

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

        Path file1 = HadoopFileTestUtils.createPath(directory.toString(),"file1.csv");
        Path file2 = HadoopFileTestUtils.createPath(directory.toString(),"file2.csv");
        HadoopFileTestUtils.createFileWithContent(fs,file1,file1Content);
        HadoopFileTestUtils.createFileWithContent(fs,file2,file2Content);

        Path outputPath = new Path(FileTestUtils.getRandomTestFilepath());
        runCascadingJob(directory,outputPath);

        String expectedContent = "0\t[header, _raw]\n" +
                "12\t[ImCESTvlhu, LOjZxHYGZy]\n" +
                "38\t[kqYkcFhbSB, RRLjuHCHze]\n" +
                "64\t[kmeEaOcKTx, mvIPrMSOSS]\n" +
                "90\t[lzzPLYFGFU, sGHTPVsYlF]\n" +
                "116\t[zpUxVlTaAq, ysoUYuyZKO]\n" +
                "153\t[fkBkKfCkuT, BRlSkqHmHe]\n" +
                "179\t[dWDJViEuot, LcdkTQBLmu]\n" +
                "205\t[ovQoDFATdn, YewByxPXqN]\n" +
                "231\t[tKBxjsSZmV, luuOivALWj]\n" +
                "257\t[mssAbiUnub, NeYnIlDMdW]\n";

        String actualResults = HadoopFileTestUtils.readMapReduceOutputFile(fs,outputPath);

        assertEquals(expectedContent,actualResults);
    }

    public Flow runCascadingJob( Path inputPath, Path outputPath) throws IOException
    {
        Properties properties = new Properties();

        ShuttlCsv inputScheme = new ShuttlCsv();
        TextLine outputScheme = new TextLine();

        Hfs input = new Hfs(inputScheme,inputPath.toString());
        Hfs output = new Hfs(outputScheme,outputPath.toString(),SinkMode.REPLACE);

        Pipe pipe = new Pipe( "test" );
        Flow flow = new HadoopFlowConnector( properties ).connect( input, output, pipe );

        flow.complete();

        return flow;
    }
}