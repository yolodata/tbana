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

package com.yolodata.tbana.hadoop.mapred.splunk;

import com.yolodata.tbana.hadoop.mapred.shuttl.ShuttlInputFormatConstants;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;

public class SplunkDataQueryFactory {

    public static SplunkDataQuery createWithJobConf(JobConf jobConf) {
        String earliest = jobConf.get(ShuttlInputFormatConstants.EARLIEST_TIME);
        String latest = jobConf.get(ShuttlInputFormatConstants.LATEST_TIME);
        String indexes = jobConf.get(ShuttlInputFormatConstants.INDEX_LIST);
        String [] indexList = indexes.split(SplunkDataQuery.INDEX_LIST_SEPARATOR);

        return new SplunkDataQuery(DateTime.parse(earliest), DateTime.parse(latest), indexList);
    }
}
