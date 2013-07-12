package com.yolodata.tbana.util.search;

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.util.search.filter.ExtensionFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import com.yolodata.tbana.testutils.FileTestUtils;
import com.yolodata.tbana.testutils.HadoopFileTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class HadoopPathFinderTest {

    private PathFinder finder;
    private LocalFileSystem fileSystem;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
        finder = new HadoopPathFinder(fileSystem);
    }

    @Test
    public void testFindPathsNoFilter() throws Exception {
        String pathToFile = FileTestUtils.getPathToTestFile("file.ext");
        HadoopFileTestUtils.createFileWithContent(fileSystem,new Path(pathToFile),"empty");

        List<String> results = finder.findPaths(pathToFile);

        assertEquals(1,results.size());
        assertEquals(pathToFile,results.get(0));
    }

    @Test
    public void testFindPathsWithFilters() throws Exception {

        String [] filesToCreate = new String[] {"file.csv","file.tsv","file.ext"};
        Path directory = FileSystemTestUtils.createDirectoryWithEmptyFiles(fileSystem,filesToCreate);

        String extension = ".ext";
        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new ExtensionFilter(extension));

        List<String> results = finder.findPaths(directory.toString(),filters);

        assertEquals(1,results.size());
        assertTrue(results.get(0).endsWith(extension));
    }

    @Test
    public void testFindPathsWithMaxResultsParameter() throws Exception {
        String [] filesToCreate = new String[] {"file.csv","file.tsv","file.ext"};

        Path directory = FileSystemTestUtils.createDirectoryWithEmptyFiles(fileSystem,filesToCreate);
        HadoopPathFinder finderWithMaxResults = new HadoopPathFinder(fileSystem,2);

        List<String> paths = finderWithMaxResults.findPaths(directory.toString());

        assertEquals(2, paths.size());
    }
}
