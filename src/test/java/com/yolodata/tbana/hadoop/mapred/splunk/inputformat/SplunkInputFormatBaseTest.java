package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.TestConfigurations;
import com.yolodata.tbana.TestUtils;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

public abstract class SplunkInputFormatBaseTest {

    private Path outputPath= new Path("build/testTMP/"+ getMethodToTest());
    private FileSystem fs;
    private Configuration testConfiguration= null;

    protected abstract String getMethodToTest();

    @Before
    public void setUp() throws Exception {
        fs= FileSystem.get(new Configuration());
        fs.delete(outputPath, true);

        testConfiguration= TestConfigurations.getConfigurationWithSplunkConfigured();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInputFormatMultipleSplits() throws Exception {
        testConfiguration.setInt(SplunkInputFormat.INPUTFORMAT_SPLITS, 4);

        testInputFormat(testConfiguration, 5);
    }

    @Test
    public void testInputFormatDefaultConfig() throws Exception {

        testInputFormat(testConfiguration, 5);

    }

    @Test
    public void testEmptySearchResults() throws Exception {
        testConfiguration.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search nothing");

        testInputFormat(testConfiguration, -1);
    }

    @Test
    public void testEmptySearchResultsWithMultipleSplits() throws Exception {
        testConfiguration.set(SplunkConf.SPLUNK_SEARCH_QUERY, "search nothing");
        testConfiguration.setInt(SplunkInputFormat.INPUTFORMAT_SPLITS, 4);

        testInputFormat(testConfiguration, -1);
    }

    private void testInputFormat(Configuration conf, int numberOfExpectedResults) throws Exception {
        boolean jobCompleted = runJob(conf);
        assert(jobCompleted == true); // Means that the job successfully finished

        String outputContent = TestUtils.readMapReduceOutputFile(fs, outputPath);

        List<String> lines = TestUtils.getLinesFromString(outputContent);
        int actualEvents = lines.size()-1; //Remove one line due to header

        assert(actualEvents == numberOfExpectedResults);

        List<String> expectedEndOfLines= FileUtils.readLines(new File("build/resources/test/splunkMockData.txt"));

        // Check that the last column of each line ends with the expected values
        for(int i=0;i<lines.size();i++)
            assert(lines.get(i).endsWith(expectedEndOfLines.get(i)));
    }

    private boolean runJob(Configuration conf) throws Exception {
        return (ToolRunner.run(conf, new SplunkTestRunner(conf),
                new String[]{getMethodToTest(),outputPath.toString()}) == 0);
    }
}

