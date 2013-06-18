package com.yolodata.tbana.hadoop.mapred;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVReaderTest {

    private CSVReader csvReader;

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File(TestUtils.TEST_FILE_PATH));
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(TestUtils.TEST_FILE_PATH));
    }

    @Test
    public void readLine_emptyFile_returnsEmptyList() throws IOException {
        String csvContent = "";
        ArrayList<Text> actual = new ArrayList<Text>();
        ArrayList<Text> expected = new ArrayList<Text>();
        testFileContent(csvContent, actual, expected);
    }


    @Test
    public void readLine_oneLine_returnsOneText() throws IOException {
        String csvContent = "hello";
        ArrayList<Text> actual = new ArrayList<Text>();
        ArrayList<Text> expected = new ArrayList<Text>();
        expected.add(new Text("hello"));
        testFileContent(csvContent, actual, expected);
    }

    private void testFileContent(String csvContent, List<Text> actual, List<Text> expected) throws IOException {
        String filepath = TestUtils.getRandomTestFilepath();
        TestUtils.createFileWithContent(filepath, csvContent);

        csvReader = new CSVReader(new FileReader(filepath));

        csvReader.readLine(actual);

        assert(actual.size() == expected.size());

        for(int i=0; i<actual.size(); i++)
            assert(actual.get(i).equals(expected.get(i)));

    }
}
