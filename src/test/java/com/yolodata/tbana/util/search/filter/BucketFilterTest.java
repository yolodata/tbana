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

package com.yolodata.tbana.util.search.filter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BucketFilterTest {

    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = mock(FileSystem.class);
    }

    @Test
    public void testFilterCorrectBucketFormat() throws Exception {
        Path bucket = new Path("path/to/bucket/db_1000_900_0");

        BucketFilter bucketFilter = new BucketFilter(fileSystem);
        SearchFilter dependency = mock(SearchFilter.class);
        when(dependency.accept(anyString())).thenReturn(true);

        assertEquals(true, bucketFilter.accept(bucket, dependency));
    }

    @Test
    public void testFilterBadlyFormattedBuckets() throws Exception {
        Path badBucket = new Path("path/to/bucket/db_a_b");

        BucketFilter bucketFilter = new BucketFilter(fileSystem);
        SearchFilter dependency = mock(SearchFilter.class);
        when(dependency.accept(anyString())).thenReturn(true);

        assertEquals(false, bucketFilter.accept(badBucket,dependency));
    }


}
