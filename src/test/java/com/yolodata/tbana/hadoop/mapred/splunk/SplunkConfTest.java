package com.yolodata.tbana.hadoop.mapred.splunk;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

public class SplunkConfTest {

    @Test
    public void testKeyExistsWithNonExistingKey() {
        String key = "key.that.does.not.exist";
        SplunkConf conf = getConfWithoutKey(key);

        assert(!conf.keyExists(key));
    }

    @Test
    public void testKeyExistsWithExistingKey() {
        String key = "key.that.exists";
        SplunkConf conf = getConfWithKey(key);

        assert(conf.keyExists(key));
    }

    @Test (expected=SplunkConfigurationException.class)
    public void testRequireKeyInConfigurationWithANonExistingKey() {
        String key = "key.that.does.not.exist";
        SplunkConf conf = getConfWithoutKey(key);

        conf.requireKeyInConfiguration(key);
    }

    @Test
    public void testRequireKeyInConfigurationWithExistingKey() {

        String key = "key.that.exists";
        SplunkConf conf = getConfWithKey(key);
        assert(conf.keyExists(key));
    }

    private String anyString() {
        return RandomStringUtils.randomAlphabetic(5);
    }

    private SplunkConf getConfWithKey(String key) {
        SplunkConf conf = new SplunkConf();
        conf.set(key,anyString());
        assert(conf.get(key) != null);

        return conf;
    }

    private SplunkConf getConfWithoutKey(String key) {
        SplunkConf conf = new SplunkConf();
        assert(conf.get(key) == null);

        return conf;
    }

}
