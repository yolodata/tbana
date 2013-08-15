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

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.hadoop.conf.Configuration;

public class SplunkService {

    public static Service connect(SplunkConf configuration) {
        return connect(
                configuration.get(SplunkConf.SPLUNK_USERNAME),
                configuration.get(SplunkConf.SPLUNK_PASSWORD),
                configuration.get(SplunkConf.SPLUNK_HOST),
                configuration.getInt(SplunkConf.SPLUNK_PORT, 8080));
    }

    public static Service connect(SplunkConf configuration, String host, int port) {
        return connect(
                configuration.get(SplunkConf.SPLUNK_USERNAME),
                configuration.get(SplunkConf.SPLUNK_PASSWORD),
                host,
                port);
    }

    public static Service connect(String username, String password, String host, int port ) {
        ServiceArgs loginArgs = getLoginArgs(
                username,
                password,
                host,
                port);

        return Service.connect(loginArgs);
    }

    private static ServiceArgs getLoginArgs(String username, String password, String host, int port) {
        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername(username);
        loginArgs.setPassword(password);
        loginArgs.setHost(host);
        loginArgs.setPort(port);
        return loginArgs;

    }
}
