package com.yolodata.tbana.util.search.filter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModifiedTimeFilterTest {

    @Test
    public void testFilter() throws Exception {

        FileSystem fs = mock(FileSystem.class);

        // We want everything newer than 100, i.e. modified times over 100 are accepted
        ModifiedTimeFilter filter = new ModifiedTimeFilter(fs, 100);

        Path anyPath = createMockPathWithModifiedTime(fs, 1000);
        assertEquals(true, filter.accept(anyPath));

        anyPath = createMockPathWithModifiedTime(fs, 90);
        assertEquals(false, filter.accept(anyPath));

        anyPath = createMockPathWithModifiedTime(fs, 100);
        assertEquals(false, filter.accept(anyPath));
    }

    private Path createMockPathWithModifiedTime(FileSystem fs, long modifiedTime) throws IOException {
        Path anyPath = new Path("anyPath");
        FileStatus status = new FileStatus(0,false,0,0,modifiedTime,anyPath);
        when(fs.getFileStatus(anyPath)).thenReturn(status);
        return anyPath;
    }
}
