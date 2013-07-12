package com.yolodata.tbana.hadoop.mapred.shuttl.bucket.search;


import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.shuttl.bucket.BucketFinder;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BucketFinderTest {

    private FileSystem fs;
    private Path root;

    @Before
    public void setUp() throws Exception {
        fs = FileSystem.getLocal(new Configuration());

        fs.delete(new Path(TestUtils.TEST_FILE_PATH),true);

        root = FileSystemTestUtils.createEmptyDir(fs);

        String [] directories = {"db_28800000_28810000_index","db_28810001_28820000_index","db_28820001_28830000_index"};
        FileSystemTestUtils.createDirectories(fs,root,directories);
    }

    @Test
    public void testGetBucketsByTimeRange() throws Exception {

        String earliestTime = "1970-01-01 00:00:00";
        String latestTime = "1970-01-01 00:00:10";

        Index index = new Index(root.toString(),root.getName());
        BucketFinder bucketFinder = new BucketFinder(fs, index);
        List<Bucket> buckets = bucketFinder.search(new SplunkDataQuery(earliestTime, latestTime));

        assertEquals(1, buckets.size());
    }

    @Test
    public void testGetBucketsWithMaxResults() throws Exception {
        Index index = new Index(root.toString(),root.getName());
        BucketFinder bucketFinder = new BucketFinder(fs, index, 1);
        List<Bucket> buckets = bucketFinder.search(new SplunkDataQuery());

        assertEquals(1, buckets.size());
    }
}
