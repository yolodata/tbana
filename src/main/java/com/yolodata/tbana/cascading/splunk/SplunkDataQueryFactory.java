package com.yolodata.tbana.cascading.splunk;

import com.yolodata.tbana.hadoop.mapred.shuttl.ShuttlInputFormatConstants;
import org.apache.hadoop.mapred.JobConf;

public class SplunkDataQueryFactory {

    public static SplunkDataQuery createWithJobConf(JobConf jobConf) {
        String earliest = jobConf.get(ShuttlInputFormatConstants.EARLIEST_TIME);
        String latest = jobConf.get(ShuttlInputFormatConstants.LATEST_TIME);
        String indexes = jobConf.get(ShuttlInputFormatConstants.INDEX_LIST);
        String [] indexList = indexes.split(SplunkDataQuery.INDEX_LIST_SEPARATOR);

        return new SplunkDataQuery(earliest,latest,indexList);
    }
}
