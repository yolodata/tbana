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
