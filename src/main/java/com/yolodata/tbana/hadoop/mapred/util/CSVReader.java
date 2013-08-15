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

package com.yolodata.tbana.hadoop.mapred.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class CSVReader implements Closeable {


    private final String delimiter = "\"";
    private final String separator = ",";
    private final Reader reader;


    public CSVReader(Reader reader) {
        this.reader = reader;
    }

    public int readLine(List<Text> values) throws IOException {
        values.clear();

        StringBuffer line = new StringBuffer();
        int charactersRead = 0;
        char currentChar;

        boolean insideQuotes = false;
        int i, quoteOffset = 0, delimiterOffset = 0;


        while ((i = reader.read()) != -1) {
            currentChar = (char) i;
            charactersRead++;
            line.append(currentChar);
            // Check quotes, as delimiter inside quotes don't count
            if (currentChar == delimiter.charAt(quoteOffset)) {
                quoteOffset++;
                if (quoteOffset >= delimiter.length()) {
                    insideQuotes = !insideQuotes;
                    quoteOffset = 0;
                }
            } else {
                quoteOffset = 0;
            }
            // Check delimiters, but only those outside of quotes
            if (!insideQuotes) {
                if (currentChar == separator.charAt(delimiterOffset)) {
                    delimiterOffset++;
                    if (delimiterOffset >= separator.length()) {
                        foundDelimiter(line, values, true);
                        delimiterOffset = 0;
                    }
                } else {
                    delimiterOffset = 0;
                }
                // A new line outside of a quote is a real csv line breaker
                if (currentChar == '\n') {
                    break;
                }
            }
        }
        foundDelimiter(line, values, false);
        return charactersRead;
    }

    protected void foundDelimiter(StringBuffer sb, List<Text> values, boolean takeDelimiterOut)
            throws UnsupportedEncodingException {
        Text text = new Text();
        String val = (takeDelimiterOut) ? sb.substring(0, sb.length() - separator.length()) : sb.toString();
        val = StringUtils.strip(val,"\n");
        val = StringUtils.strip(val,delimiter);

        text.set(val);
        values.add(text);

        sb.setLength(0);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
