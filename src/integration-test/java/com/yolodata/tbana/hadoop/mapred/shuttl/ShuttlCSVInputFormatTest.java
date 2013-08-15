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

package com.yolodata.tbana.hadoop.mapred.shuttl;

import com.yolodata.tbana.hadoop.mapred.FileContentProvider;
import com.yolodata.tbana.testutils.FileTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
import com.yolodata.tbana.util.search.ShuttlDirectoryTreeFactory;
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
    private ShuttlDirectoryTreeFactory directoryTreeFactory;

    @Before
    public void setUp() throws IOException {
        fs = FileSystem.get(new Configuration());
        fs.delete(new Path(TestUtils.TEST_FILE_PATH),true);
        directoryTreeFactory = new ShuttlDirectoryTreeFactory();
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(new Path(TestUtils.TEST_FILE_PATH), true);
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
        Path index = directoryTreeFactory.addIndex(directoryTreeFactory.getIndexerPaths().get(0),"Index1");
        Path bucket = directoryTreeFactory.addBucket(index,"db_0_1_idx");

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

        int [] offsets = new int [] {12,38,64,90,116};
        Path file1 = HadoopFileTestUtils.createPath(bucket.toString(),"file1.csv");
        Path file2 = HadoopFileTestUtils.createPath(bucket.toString(),"file2.csv");

        HadoopFileTestUtils.createFileWithContent(fs,file1,file1Content);
        HadoopFileTestUtils.createFileWithContent(fs,file2,file2Content);


        Path outputPath = new Path(FileTestUtils.getRandomTestFilepath());
        assertTrue(runJob(new Configuration(), new String[] {directoryTreeFactory.getRoot().toString(),outputPath.toString()}));

        String expectedContent = "0\theader,_raw\n" +
                "12\tImCESTvlhu,LOjZxHYGZy\n" +
                "38\tkqYkcFhbSB,RRLjuHCHze\n" +
                "64\tkmeEaOcKTx,mvIPrMSOSS\n" +
                "90\tlzzPLYFGFU,sGHTPVsYlF\n" +
                "116\tzpUxVlTaAq,ysoUYuyZKO\n" +
                "153\tfkBkKfCkuT,BRlSkqHmHe\n" +
                "179\tdWDJViEuot,LcdkTQBLmu\n" +
                "205\tovQoDFATdn,YewByxPXqN\n" +
                "231\ttKBxjsSZmV,luuOivALWj\n" +
                "257\tmssAbiUnub,NeYnIlDMdW\n";

        String actualResults = HadoopFileTestUtils.readMapReduceOutputFile(fs,outputPath);

        assertEquals(expectedContent,actualResults);
    }

    private String combineContent(String file1Content, String file2ContentWithoutHeader) {
        return file1Content.concat(file2ContentWithoutHeader);
    }

    private void runTestOnContent(String content, int[] offsets) throws Exception {

        Path index = directoryTreeFactory.addIndex(directoryTreeFactory.getIndexerPaths().get(0),"Index1");
        Path bucket = directoryTreeFactory.addBucket(index,"db_0_1_idx");

        Path inputPath = HadoopFileTestUtils.createPath(bucket.toString(),FileTestUtils.getRandomFilename("csv"));
        HadoopFileTestUtils.createFileWithContent(fs, inputPath, content);

        Path outputPath = new Path(FileTestUtils.getRandomTestFilepath());
        assertTrue(runJob(new Configuration(), new String[] {directoryTreeFactory.getRoot().toString(),outputPath.toString()}));

        String result = HadoopFileTestUtils.readMapReduceOutputFile(fs,outputPath);

        String expected = getExpectedResult(content, offsets);

        assertEquals(expected,result);
    }

    private String getExpectedResult(String content, int[] offsets) throws IOException {
        List<String> linesFromExpected = TestUtils.getLinesFromString(content);
        addOffsetToEachLine(linesFromExpected, offsets);
        String expected = "";
        for(int i=0;i<linesFromExpected.size();i++)
            if(i<linesFromExpected.size())
                expected = expected.concat(linesFromExpected.get(i)+"\n");
            else
                expected = expected.concat(linesFromExpected.get(i));
        return expected;
    }


    private void addOffsetToEachLine(List<String> linesFromExpected, int [] offsets) {
        for(int i=0;i<linesFromExpected.size();i++)
            linesFromExpected.set(i, (offsets[i] + "\t").concat(linesFromExpected.get(i)));
    }

    private boolean runJob(Configuration conf, String [] args) throws Exception {
        return (ToolRunner.run(conf, new ShuttlTestJob(conf),args) == 0);
    }


}
