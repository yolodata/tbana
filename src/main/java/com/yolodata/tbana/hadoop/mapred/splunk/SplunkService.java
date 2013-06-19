package com.yolodata.tbana.hadoop.mapred.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.hadoop.conf.Configuration;

public class SplunkService {

    public static Service connect(Configuration configuration) {
        return Service.connect(getLoginArgs(configuration));
    }

    private static ServiceArgs getLoginArgs(Configuration configuration) {
        ServiceArgs loginArgs = new ServiceArgs();
        loginArgs.setUsername(configuration.get(SplunkConf.SPLUNK_USERNAME));
        loginArgs.setPassword(configuration.get(SplunkConf.SPLUNK_PASSWORD));
        loginArgs.setHost(configuration.get(SplunkConf.SPLUNK_HOST));
        loginArgs.setPort(configuration.getInt(SplunkConf.SPLUNK_PORT, 8080));

        return loginArgs;

    }
}
