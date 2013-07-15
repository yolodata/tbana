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
