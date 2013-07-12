/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.cascading.splunk;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import com.yolodata.tbana.hadoop.mapred.splunk.SplunkConfigurationException;
import com.yolodata.tbana.testutils.TestConfigurations;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SplunkTapTest {

    private SplunkScheme inputScheme;
    private Properties properties;

    @Before
    public void setUp() throws Exception {
        inputScheme = new SplunkScheme(TestConfigurations.getSplunkSearch());
        properties = TestConfigurations.getSplunkLoginAsProperties();
    }

    @Test
    public void testSplunkTap()
    {
        Tap tap = new SplunkTap(properties, inputScheme);
        FlowProcess flowProcess = mock(FlowProcess.class);

        tap.sourceConfInit(flowProcess, new JobConf());
    }

    @Test(expected=SplunkConfigurationException.class)
    public void testSplunkTapWithInvalidConfig() throws Exception {
        Properties emptyProperties = new Properties();

        Tap tap = new SplunkTap(emptyProperties, inputScheme);
        FlowProcess flowProcess = mock(FlowProcess.class);

        tap.sourceConfInit(flowProcess, new JobConf());
    }

    @Test
    public void testSetConfKey() {

        String key = "Some.Unique.key";
        String value = "SomeValue";

        Properties propsWithKey = new Properties();
        propsWithKey.put(key, value);

        SplunkTap tap = new SplunkTap(propsWithKey,inputScheme);
        JobConf conf = new JobConf();


        assertEquals(null, conf.get(key));
        tap.setConfKey(conf,key);
        assertEquals(value, conf.get(key));
    }
}