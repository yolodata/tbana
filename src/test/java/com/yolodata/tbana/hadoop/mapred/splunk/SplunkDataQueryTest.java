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
import org.junit.Test;

public class SplunkDataQueryTest {

    @Test
    public void testDefaultConstructor(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery();
        assert splunkDataQuery.getSplunkQuery().equals("search index=*");
        assert splunkDataQuery.getIndexesString().equals("*");
    }

    @Test
    public void testConstructorWithIndexes(){
        SplunkDataQuery splunkDataQuery= new SplunkDataQuery(new DateTime(0), DateTime.now(), new String[] {"main", "_internal"});
        assert splunkDataQuery.getSplunkQuery().equals("search index=main OR index=_internal");
        assert splunkDataQuery.getIndexesString().equals("main,_internal");
    }
}
