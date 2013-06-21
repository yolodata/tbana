package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.TestUtils;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public abstract class SplunkInputFormatBaseTest {

    private Path outputPath= new Path("build/testTMP/"+ getMethodToTest());
    private FileSystem fs;

    protected abstract String getMethodToTest();

    @Before
    public void setUp() throws Exception {
        fs= FileSystem.get(new Configuration());
        fs.delete(outputPath, true);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInputFormatMultipleSplits() throws Exception {
        Configuration conf = new Configuration();
        conf.setInt(SplunkInputFormat.INPUTFORMAT_SPLITS, 4);

        testInputFormat(conf);
    }

    @Test
    public void testInputFormatDefaultConfig() throws Exception {

        testInputFormat(new Configuration());

    }
    private void testInputFormat(Configuration conf) throws Exception {
        boolean jobCompleted = runJob(conf);
        assert(jobCompleted == true); // Means that the job successfully finished

        String outputContent = TestUtils.readMapReduceOutputFile(fs, outputPath);

        List<String> lines = TestUtils.getLinesFromString(outputContent);
        int actualEvents = lines.size()-1; //Remove one line due to header

        assert(actualEvents == 5);

        String[] expectedEndOfLines = {
                "_raw",
                "#<DateTime 2012-12-31T23:59:59.000Z> count=0",
                "#<DateTime 2012-12-31T23:59:58.000Z> count=1",
                "#<DateTime 2012-12-31T23:59:57.000Z> count=2",
                "#<DateTime 2012-12-31T23:59:56.000Z> count=3",
                "#<DateTime 2012-12-31T23:59:55.000Z> count=4"};

        // Check that the last column of each line ends with the expected values
        for(int i=0;i<lines.size();i++)
            assert(lines.get(i).endsWith(expectedEndOfLines[i]));
    }

    private boolean runJob(Configuration conf) throws Exception {
        return (ToolRunner.run(conf, new SplunkTestRunner(conf),
                new String[]{getMethodToTest(),outputPath.toString()}) == 0);
    }
}

