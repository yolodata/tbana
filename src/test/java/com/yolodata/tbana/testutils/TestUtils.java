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

package com.yolodata.tbana.testutils;

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