package com.yolodata.tbana.util.search.filter;

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BucketFilterTest {

    @Test
    public void testFilter() throws Exception {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        Path testRoot = FileSystemTestUtils.createEmptyDir(fileSystem);

        String [] bucketNames = {"db_1000_2000","a_b_c","raw"};
        List<Path> bucketPaths = FileSystemTestUtils.createDirectories(fileSystem, testRoot, bucketNames);

        SearchFilter bucketFilter = new BucketFilter(fileSystem);

        assertEquals(true, bucketFilter.accept(bucketPaths.get(0).toString()));
        assertEquals(false, bucketFilter.accept(bucketPaths.get(1).toString()));
        assertEquals(false, bucketFilter.accept(bucketPaths.get(2).toString()));
    }
}
