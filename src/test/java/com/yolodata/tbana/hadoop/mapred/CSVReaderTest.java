package com.yolodata.tbana.hadoop.mapred;

import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
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
    private ArrayList<Text> actual;
    private ArrayList<Text> expected;

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File(TestUtils.TEST_FILE_PATH));
        FileUtils.forceMkdir(new File(TestUtils.TEST_FILE_PATH));
        actual = new ArrayList<Text>();
        expected = new ArrayList<Text>();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(TestUtils.TEST_FILE_PATH));
    }

    @Test
    public void readLine_emptyFile_returnsEmptyText() throws IOException {
        String csvContent = "";
        expected.add(new Text());
        testFileContent(csvContent, expected);
    }


    @Test
    public void readLine_oneColumn_returnsOneText() throws IOException {
        String csvContent = "hello";
        expected.add(new Text("hello"));
        testFileContent(csvContent, expected);
    }

    @Test
    public void readLine_twoColumns_returnsTwoTexts() throws IOException {
        String csvContent = "hello,world";

        expected.add(new Text("hello"));
        expected.add(new Text("world"));
        testFileContent(csvContent,expected);
    }

    private void testFileContent(String csvContent, List<Text> expected) throws IOException {
        String filepath = TestUtils.getRandomTestFilepath();

        assert(TestUtils.createFileWithContent(filepath, csvContent));

        csvReader = new CSVReader(new FileReader(filepath));

        csvReader.readLine(actual);

        assert(actual.size() == expected.size());

        for(int i=0; i<actual.size(); i++)
            assert(actual.get(i).equals(expected.get(i)));

    }
}
