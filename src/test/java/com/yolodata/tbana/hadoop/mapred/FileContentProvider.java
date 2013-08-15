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

package com.yolodata.tbana.hadoop.mapred;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

public class FileContentProvider {

    public static String getRandomContent(String [] header, int lines) {

        return getMultilineRandomContent(header,lines,0);
    }

    public static String getMultilineRandomContent(String [] header, int lines, int multiLines) {
        StringBuilder result = getStringBuilderWithHeader(header);

        for(int i=0;i<lines;i++) {
            addLineWithMultiline(header, multiLines, result);
            result.append("\n");
        }

        return result.toString();
    }

    private static void addLineWithMultiline(String[] header, int multiLines, StringBuilder result) {
        for(int j=0;j<header.length;j++) {
            addColumnWithMultiline(result, multiLines);
            if(j<header.length-1)
                result.append(',');
        }
    }

    private static void addColumnWithMultiline(StringBuilder result, int multiLines) {
        result.append("\"");
        for(int i=0;i<multiLines;i++)
        {
            result.append(RandomStringUtils.randomAlphabetic(10));
            result.append("\n");
        }
        result.append(RandomStringUtils.randomAlphabetic(10));
        result.append("\"");
    }

    private static StringBuilder getStringBuilderWithHeader(String[] header) {
        StringBuilder result = new StringBuilder();
        result.append(StringUtils.join(header, ","));
        result.append("\n");
        return result;
    }

}
