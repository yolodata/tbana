package com.yolodata.tbana.testutils;

import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.net.URI;

public class FileTestUtils {


    public static String getPathToTestFile(String filename) {
        return TestUtils.TEST_FILE_PATH.concat(filename);
    }

    public static String getRandomTestFilepath() {
        return getPathToTestFile(getRandomFilename());
    }

    private static String getRandomFilename() {
        return RandomStringUtils.randomAlphanumeric(25);
    }

    public static String getRandomFilename(String extension) {
        return RandomStringUtils.randomAlphanumeric(25).concat("."+extension);
    }

    public static boolean createFileWithContent(String filepath, String content) throws IOException {
        File f = new File(filepath);
        if(!f.createNewFile())
            return false;

        PrintWriter pw = new PrintWriter(f);
        pw.write(content);
        pw.close();

        return true;
    }

    public static String readContentFromLocalFile(URI path) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(path)));
        String content = org.apache.tools.ant.util.FileUtils.readFully(inputStreamReader);
        inputStreamReader.close();

        return content;
    }
}
