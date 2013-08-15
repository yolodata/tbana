/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.hadoop.mapred.splunk.inputformat;

import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConf;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkInputFormat;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import com.yolodata.tbana.testutils.TestConfigurations;
import com.yolodata.tbana.testutils.TestUtils;
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

import static org.junit.Assert.assertEquals;

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

        String outputContent = HadoopFileTestUtils.readMapReduceOutputFile(fs, outputPath);

        List<String> lines = TestUtils.getLinesFromString(outputContent);
        int actualEvents = lines.size()-1; //Remove one line due to header

        assertEquals(numberOfExpectedResults, actualEvents);

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

