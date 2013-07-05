package com.yolodata.tbana.testutils;

import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.tools.ant.util.FileUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static final String TEST_FILE_PATH = "build/testTMP/";

    public static String readMapReduceOutputFile(FileSystem fs, Path outputPath) throws IOException, IllegalAccessException, InstantiationException {
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        StringBuilder sb = new StringBuilder();
        for (FileStatus f : fileStatuses) {
            if (f.getPath().toString().endsWith("_SUCCESS"))
                continue; // skip SUCCESS file
            String partFileContent = readContentFromLocalFile(f.getPath().toUri());
            if (partFileContent != null)
                sb.append(partFileContent);
        }

        return sb.toString();
    }

    public static String readContentFromLocalFile(URI path) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(path)));
        String content = FileUtils.readFully(inputStreamReader);
        inputStreamReader.close();

        return content;
    }

    public static void createFileWithContent(FileSystem fs, Path path, String content) throws IOException {
        FSDataOutputStream fso = fs.create(path, true);
        fso.writeBytes(content);
        fso.flush();
        fso.close();
    }

    public static String getPathToTestFile(String filename) {
        return TEST_FILE_PATH.concat(filename);
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

     public static List<String> getLinesFromString(String outputContent) throws IOException {
         List<String> result = new ArrayList<String>();

         StringReader reader = new StringReader(outputContent);
         CSVReader csvReader = new CSVReader(reader);

         List<Text> textList = new ArrayList<Text>();
         StringBuilder sb = new StringBuilder();
         while(csvReader.readLine(textList) != 0) {
             sb.setLength(0);
             for(int i=0;i<textList.size();i++) {
                 sb.append(textList.get(i).toString());
                 if(i<textList.size()-1)
                    sb.append(',');
             }
             result.add(sb.toString());
         }

         return result;

    }

    public static Path createPath(String directory, String filename) {
        if(!directory.endsWith("/"))
            directory = directory.concat("/");

        String path = directory.concat(filename);

        return new Path(path);
    }
}