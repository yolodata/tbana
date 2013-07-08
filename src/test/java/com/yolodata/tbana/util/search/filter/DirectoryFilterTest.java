package com.yolodata.tbana.util.search.filter;

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DirectoryFilterTest {

    @Test
    public void testFilterOnCorrectData() throws Exception {
        SearchFilter filter = new DirectoryFilter(FileSystem.getLocal(new Configuration()));

        Path directory = FileSystemTestUtils.createEmptyDir(FileSystem.getLocal(new Configuration()));
        Path file = FileSystemTestUtils.createEmptyFile(FileSystem.getLocal(new Configuration()));

        assertEquals(true, filter.accept(directory.toString()));
        assertEquals(false,filter.accept(file.toString()));
    }


}
