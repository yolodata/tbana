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

import org.joda.time.DateTime;

import java.io.Serializable;

public class SplunkDataQuery implements Serializable {

    public static final String INDEX_LIST_SEPARATOR = ",";

    private DateTime earliestTime;
    private DateTime latestTime;
    private String[] indexes;
    public static final String[] ALL_INDEXES= new String[] {"*"};
    public static final String[] ALL_INDEXES_INCLUDING_INTERNAL= new String[] {"*", "_*"};
    public static final DateTime ALL_TIME_EARLIEST= new DateTime(0);

    public SplunkDataQuery(){
        this(ALL_TIME_EARLIEST, DateTime.now());
    }

    public SplunkDataQuery(DateTime earliestTime, DateTime latestTime) {
        this(earliestTime, latestTime, ALL_INDEXES);
    }

    public SplunkDataQuery(DateTime earliestTime, DateTime latestTime, String[] indexes){
        if(indexes == null)
            throw new NullPointerException();
        if(indexes.length == 0)
            throw new IllegalArgumentException("Empty index list is not allowed." +
                    " Use different constructor or SplunkDataQuery.ALL_INDEXES");
        this.indexes= indexes;
        this.earliestTime= earliestTime;
        this.latestTime= latestTime;
    }

    public String getSplunkQuery() {
        return "search index=" + concatenateIndexes(" OR index=");
    }

    private String concatenateIndexes(String separator) {
        StringBuilder results= new StringBuilder(indexes[0]);
        for(int i= 1; i < indexes.length; i++){
            results.append(separator + indexes[i]);
        }
        return results.toString();
    }

    public DateTime getEarliestTime(){
        return earliestTime;
    }

    public String getEarliestTimeString(){
        if (earliestTime == ALL_TIME_EARLIEST){
            return "0";
        }
        else return earliestTime.toString();
    }

    public DateTime getLatestTime() {
        return latestTime;
    }

    public String getLatestTimeString(){
        return latestTime.toString();
    }

    public String [] getIndexes() {
        return indexes;
    }

    public String getIndexesString() {
        return concatenateIndexes(INDEX_LIST_SEPARATOR);
    }
}
