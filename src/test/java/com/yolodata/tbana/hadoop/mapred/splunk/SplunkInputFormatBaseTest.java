package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.TestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public abstract class SplunkInputFormatBaseTest {

    private Path outputPath= new Path("build/testTMP/"+getClassToTest());
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        fs= FileSystem.get(new Configuration());
        fs.delete(outputPath, true);
    }

    protected abstract String getClassToTest();


    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInputFormat() throws Exception {

        boolean jobCompleted = runJob();
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

    private boolean runJob() throws Exception {
        return (ToolRunner.run(new Configuration(), new SplunkTestRunner(),
                new String[]{this.getClass().getPackage().getName().concat("."+getClassToTest()),outputPath.toString()}) == 0);
    }
}

