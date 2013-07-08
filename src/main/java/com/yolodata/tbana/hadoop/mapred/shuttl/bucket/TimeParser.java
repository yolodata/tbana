package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeParser {

    private static Map<String,String> supportedTimePatterns = new HashMap<String, String>();

    static {
        supportedTimePatterns = new HashMap<String, String>();
        supportedTimePatterns.put("(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{1,2})\\+(\\d{0,2}):(\\d{0,4})", "yyyy-MM-dd'T'HH:mm+s:SSSS");
        supportedTimePatterns.put("(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{1,2}):(\\d{0,2})", "yyyy-MM-dd HH:mm:ss");
    }

    public static long parse(String dateString) throws ParseException {

        for(String pattern : supportedTimePatterns.keySet()) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(dateString);

            if(m.matches()) {
                DateFormat dateFormatter = new SimpleDateFormat(supportedTimePatterns.get(pattern));
                Date d = dateFormatter.parse(dateString);
                return d.getTime();
            }
        }

        return -1;
    }
}
