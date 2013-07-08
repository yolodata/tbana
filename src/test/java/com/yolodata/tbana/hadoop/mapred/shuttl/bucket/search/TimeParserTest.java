package com.yolodata.tbana.hadoop.mapred.shuttl.bucket.search;

import com.yolodata.tbana.hadoop.mapred.shuttl.bucket.TimeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeParserTest {

    // Format: YYYY-MM-DD'T'HH:ii+ss:SSSS
    @Test
    public void testParseFormatOne() throws Exception {
        String date1 = "2011-11-11T22:33+0:400";

        long time = TimeParser.parse(date1);

        assertEquals(time,1321079580400L);
    }

    // Format: YYYY-MM-DD HH:ii:ss
    @Test
    public void testParseFormatTwo() throws Exception {
        String date = "2011-01-01 00:00:00";

        long time = TimeParser.parse(date);

        assertEquals(time,1293868800000L);
    }

    // Format: timestamp
    @Test
    public void testParseFormatThree() throws Exception {
        String date = "2011-01-01 00:00:00";

        long time = TimeParser.parse(date);

        assertEquals(time,1293868800000L);
    }
}
