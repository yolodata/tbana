package com.yolodata.tbana.hadoop.mapred.shuttl.index;

import com.yolodata.tbana.util.search.ShuttlDirectoryTreeFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class IndexFinderTest {

    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
    }

    @Test
    public void testFinder() throws Exception {

        ShuttlDirectoryTreeFactory directoryTreeFactory = new ShuttlDirectoryTreeFactory();
        List<Path> indexers = directoryTreeFactory.getIndexerPaths();

        directoryTreeFactory.addIndex(indexers.get(0),"index1");
        directoryTreeFactory.addIndex(indexers.get(1),"index2");

        IndexFinder finder = new IndexFinder(fileSystem,directoryTreeFactory.getRoot());

        List<Index> indexes = finder.find();


        assertEquals(indexes.size(), 2);
        assertEquals(indexes.get(0).getName(), "index1");
        assertEquals(indexes.get(1).getName(), "index2");
    }
}
