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

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BucketTimestampFilterTest {

    @Test
    public void testFilter() throws Exception {

        FileSystem fileSystem = FileSystem.getLocal(new Configuration());

        String [] buckets = {"db_3000_0_index","db_1501_1500_index","db_5000_4000_index","db_3000_1500_index", "db_3000_1500_index", "db_2000_1000_index"};

        Path testRoot = FileSystemTestUtils.createEmptyDir(fileSystem);
        List<Path> bucketPaths = FileSystemTestUtils.createDirectories(fileSystem,testRoot,buckets);

        long earliest = 1000;
        long latest = 2000;

        BucketTimestampFilter filter = new BucketTimestampFilter(fileSystem,earliest,latest);

        assertEquals(true, filter.accept(bucketPaths.get(0).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(1).toString()));
        assertEquals(false, filter.accept(bucketPaths.get(2).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(3).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(4).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(5).toString()));
    }
}
