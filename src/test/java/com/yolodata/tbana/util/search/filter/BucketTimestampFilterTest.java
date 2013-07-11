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

        String [] buckets = {"db_0_3000_index","db_1500_1501_index","db_4000_5000_index","db_1500_3000_index", "db_1500_3000_index", "db_1000_2000_index"};

        Path testRoot = FileSystemTestUtils.createEmptyDir(fileSystem);
        List<Path> bucketPaths = FileSystemTestUtils.createDirectories(fileSystem,testRoot,buckets);

        long earliest = 2000;
        long latest = 1000;

        BucketTimestampFilter filter = new BucketTimestampFilter(fileSystem,earliest,latest);

        assertEquals(true, filter.accept(bucketPaths.get(0).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(1).toString()));
        assertEquals(false, filter.accept(bucketPaths.get(2).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(3).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(4).toString()));
        assertEquals(true, filter.accept(bucketPaths.get(5).toString()));
    }
}
