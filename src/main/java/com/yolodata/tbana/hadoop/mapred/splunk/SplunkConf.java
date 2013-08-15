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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class SplunkConf extends JobConf {
    public static final String SPLUNK_USERNAME = "splunk.username";
    public static final String SPLUNK_PASSWORD = "splunk.password";
    public static final String SPLUNK_HOST = "splunk.host";
    public static final String SPLUNK_PORT = "splunk.port";
    public static final String SPLUNK_SEARCH_QUERY = "splunk.search.query";
    public static final String SPLUNK_EARLIEST_TIME = "splunk.search.earliest_time";
    public static final String SPLUNK_LATEST_TIME = "splunk.search.latest_time";
    public static final String SPLUNK_FIELD_LIST = "splunk.search.field_list";

    public static final String[] REQUIRED_LOGIN_PARAMS = {SPLUNK_USERNAME,SPLUNK_PASSWORD,SPLUNK_HOST,SPLUNK_PORT};
    public static final String[] REQUIRED_SEARCH_PARAMS = {SPLUNK_SEARCH_QUERY,SPLUNK_EARLIEST_TIME,SPLUNK_LATEST_TIME};

    public SplunkConf() {
        super(new JobConf());
    }

    public SplunkConf(Configuration configuration) {
        super(configuration);
    }

    public void validateConfiguration() throws SplunkConfigurationException {
        validateLoginConfiguration();
        validateSearchConfiguration();
    }

    public void validateLoginConfiguration() {
        for(String key : REQUIRED_LOGIN_PARAMS)
            requireKeyInConfiguration(key);
    }

    public void validateSearchConfiguration() {
        for(String key : REQUIRED_SEARCH_PARAMS)
            requireKeyInConfiguration(key);
    }

    public boolean keyExists(String key) {
        return this.get(key) != null;
    }

    public void requireKeyInConfiguration(String key) {
        if(!keyExists(key))
            throw new SplunkConfigurationException("Key "+key+" is required in JobConf configuration and is currently not set\n");
    }
}

