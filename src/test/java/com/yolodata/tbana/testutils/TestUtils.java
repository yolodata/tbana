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

}