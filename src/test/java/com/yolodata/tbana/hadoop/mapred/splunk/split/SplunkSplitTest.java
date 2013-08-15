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

package com.yolodata.tbana.hadoop.mapred.splunk.split;

import org.junit.Test;

public class SplunkSplitTest extends IndexerSplitTest{

    @Test
    public void testSerialization() throws Exception {
        SplunkSplit split = new SplunkSplit("jobID",0,1,false);
        SplunkSplit empty = new SplunkSplit();

        super.testSerialization(split,empty);
    }
}
