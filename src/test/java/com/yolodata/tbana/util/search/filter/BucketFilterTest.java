package com.yolodata.tbana.util.search.filter;

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BucketFilterTest {

    private FileSystem fileSystem;
    private Path testRoot;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
        testRoot = FileSystemTestUtils.createEmptyDir(fileSystem);
    }

    @Test
    public void testFilterCorrectBucketFormat() throws Exception {

        String [] bucketNames = {"db_1000_2000_index"};
        List<Path> bucketPaths = FileSystemTestUtils.createDirectories(fileSystem, testRoot, bucketNames);

        SearchFilter bucketFilter = new BucketFilter(fileSystem);

        assertEquals(true, bucketFilter.accept(bucketPaths.get(0).toString()));
    }

    @Test
    public void testFilterBadlyFormattedBuckets() throws Exception {
        String [] bucketNames = {"db_a_b"};
        List<Path> bucketPaths = FileSystemTestUtils.createDirectories(fileSystem, testRoot, bucketNames);

        SearchFilter bucketFilter = new BucketFilter(fileSystem);

        assertEquals(false, bucketFilter.accept(bucketPaths.get(0).toString()));
    }


}
