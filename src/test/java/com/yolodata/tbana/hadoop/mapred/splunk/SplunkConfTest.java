package com.yolodata.tbana.hadoop.mapred.splunk;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SplunkConfTest {

    @Test
    public void testKeyExistsWithNonExistingKey() {
        String key = "key.that.does.not.exist";
        Configuration conf = getConfWithoutKey(key);

        assert(SplunkConf.keyExists(conf,key) == false);
    }

    @Test
    public void testKeyExistsWithExistingKey() {
        String key = "key.that.exists";
        Configuration conf = getConfWithKey(key);

        assert(SplunkConf.keyExists(conf,key));
    }

    @Test (expected=SplunkConfigurationException.class)
    public void testRequireKeyInConfigurationWithANonExistingKey() {
        String key = "key.that.does.not.exist";
        Configuration conf = getConfWithoutKey(key);

        SplunkConf.requireKeyInConfiguration(conf,key);
    }

    @Test
    public void testRequireKeyInConfigurationWithExistingKey() {

        String key = "key.that.exists";
        Configuration conf = getConfWithKey(key);
        assert(SplunkConf.keyExists(conf,key));
    }

    private String anyString() {
        return RandomStringUtils.randomAlphabetic(5);
    }

    private Configuration getConfWithKey(String key) {
        Configuration conf = new Configuration();
        conf.set(key,anyString());
        assert(conf.get(key) != null);

        return conf;
    }

    private Configuration getConfWithoutKey(String key) {
        Configuration conf = new Configuration();
        assert(conf.get(key) == null);

        return conf;
    }

}
