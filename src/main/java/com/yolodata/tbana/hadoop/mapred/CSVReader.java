package com.yolodata.tbana.hadoop.mapred;

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

        char c;
        int numRead = 0;
        boolean insideQuote = false;
        StringBuffer sb = new StringBuffer();
        int i;
        int quoteOffset = 0, delimiterOffset = 0;
        // Reads each char from input stream unless eof was reached
        while ((i = reader.read()) != -1) {
            c = (char) i;
            numRead++;
            sb.append(c);
            // Check quotes, as delimiter inside quotes don't count
            if (c == delimiter.charAt(quoteOffset)) {
                quoteOffset++;
                if (quoteOffset >= delimiter.length()) {
                    insideQuote = !insideQuote;
                    quoteOffset = 0;
                }
            } else {
                quoteOffset = 0;
            }
            // Check delimiters, but only those outside of quotes
            if (!insideQuote) {
                if (c == separator.charAt(delimiterOffset)) {
                    delimiterOffset++;
                    if (delimiterOffset >= separator.length()) {
                        foundDelimiter(sb, values, true);
                        delimiterOffset = 0;
                    }
                } else {
                    delimiterOffset = 0;
                }
                // A new line outside of a quote is a real csv line breaker
                if (c == '\n') {
                    break;
                }
            }
        }
        foundDelimiter(sb, values, false);
        return numRead;
    }

    protected void foundDelimiter(StringBuffer sb, List<Text> values, boolean takeDelimiterOut)
            throws UnsupportedEncodingException {
        // Found a real delimiter
        Text text = new Text();
        String val = (takeDelimiterOut) ? sb.substring(0, sb.length() - separator.length()) : sb.toString();
        if (val.startsWith(delimiter) && val.endsWith(delimiter)) {
            val = (val.length() - (2 * delimiter.length()) > 0) ? val.substring(delimiter.length(), val.length()
                    - (2 * delimiter.length())) : "";
        }
        text.append(val.getBytes("UTF-8"), 0, val.length());
        values.add(text);
        // Empty string buffer
        sb.setLength(0);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
