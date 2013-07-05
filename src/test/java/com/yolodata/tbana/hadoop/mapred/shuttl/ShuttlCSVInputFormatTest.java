package com.yolodata.tbana.hadoop.mapred.shuttl;

import com.yolodata.tbana.testutils.FileTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
import com.yolodata.tbana.hadoop.mapred.FileContentProvider;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShuttlCSVInputFormatTest {

    FileSystem fs;

    @Before
    public void setUp() throws IOException {
        fs = FileSystem.get(new Configuration());
        fs.delete(new Path(TestUtils.TEST_FILE_PATH),true);
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(new Path(TestUtils.TEST_FILE_PATH),true);
    }

    @Test
    public void testReadSingleFileNoMultiline() throws Exception {

        String[] header = {"header", "_raw"};
        String content = FileContentProvider.getRandomContent(header, 5);
        int [] offsets = new int [] {0,12,38,64,90,116};

        runTestOnContent(content, offsets);
    }


    @Test
    public void testReadSingleFileWithMultiline() throws Exception {

        String[] header = {"header", "_raw"};
        String content = FileContentProvider.getMultilineRandomContent(header, 5, 2);
        int [] offsets = new int [] {0,12,82,152,222,292};

        runTestOnContent(content, offsets);
    }

    @Test
    public void testReadingDirectory() throws Exception {


    }

    private void runTestOnContent(String content, int[] offsets) throws Exception {

        Path inputPath = new Path(FileTestUtils.getRandomTestFilepath());
        HadoopFileTestUtils.createFileWithContent(fs, inputPath, content);

        Path outputPath = new Path(FileTestUtils.getRandomTestFilepath());
        assertTrue(runJob(new Configuration(), new String[] {inputPath.toString(),outputPath.toString()}));

        String result = HadoopFileTestUtils.readMapReduceOutputFile(fs,outputPath);

        List<String> linesFromExpected = TestUtils.getLinesFromString(content);
        addOffsetToEachLine(linesFromExpected, offsets);
        String expected = "";
        for(int i=0;i<linesFromExpected.size();i++)
            if(i<linesFromExpected.size())
                expected = expected.concat(linesFromExpected.get(i)+"\n");
            else
                expected = expected.concat(linesFromExpected.get(i));

        assertEquals(expected,result);
    }


    private void addOffsetToEachLine(List<String> linesFromExpected, int [] offsets) {
        for(int i=0;i<linesFromExpected.size();i++)
            linesFromExpected.set(i, (offsets[i] + "\t").concat(linesFromExpected.get(i)));
    }

    private boolean runJob(Configuration conf, String [] args) throws Exception {
        return (ToolRunner.run(conf, new ShuttlTestJob(conf),args) == 0);
    }


}
