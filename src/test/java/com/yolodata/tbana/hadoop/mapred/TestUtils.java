package com.yolodata.tbana.hadoop.mapred;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tools.ant.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static String readMapReduceOutputFile(FileSystem fs, Path outputPath) throws IOException, IllegalAccessException, InstantiationException {
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        StringBuilder sb = new StringBuilder();
        for (FileStatus f : fileStatuses) {
            if (f.getPath().toString().endsWith("_SUCCESS"))
                continue; // skip SUCCESS file
            sb.append(readContentFromLocalFile(f.getPath().toUri()));
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

     public static List<String> getLinesFromString(String outputContent) {
        List<String> result = new ArrayList<String>();

        StringBuilder sb = new StringBuilder();
        boolean withinQuote = false;
        for(char c : outputContent.toCharArray()) {

            if(!withinQuote && (c == '\n')) {
                result.add(sb.toString());
                sb.setLength(0);
                continue;
            }

            if(c == '\"') {
                withinQuote = !withinQuote;
            }

            sb.append(c);

        }
        return result;
    }
}