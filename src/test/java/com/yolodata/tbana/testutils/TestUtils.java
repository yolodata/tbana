package com.yolodata.tbana.testutils;

import com.yolodata.tbana.hadoop.mapred.util.ArrayListTextWritable;
import com.yolodata.tbana.hadoop.mapred.util.CSVReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static final String TEST_FILE_PATH = "build/testTMP/";

     public static List<String> getLinesFromString(String outputContent) throws IOException {
         List<String> result = new ArrayList<String>();

         StringReader reader = new StringReader(outputContent);
         CSVReader csvReader = new CSVReader(reader);

         ArrayListTextWritable textList = new ArrayListTextWritable();
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