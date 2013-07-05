package com.yolodata.tbana.hadoop.mapred.shuttl;

import com.yolodata.tbana.testutils.FileSystemTestUtils;
import com.yolodata.tbana.testutils.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileFinderTest {

    FileSystem fileSystem;
    FileFinder finder;

    @Before
    public void setUp() throws Exception {
        fileSystem = FileSystem.getLocal(new Configuration());
        finder = new FileFinder(fileSystem);

        fileSystem.delete(new Path(TestUtils.TEST_FILE_PATH),true);
    }

    @Test
    public void testGetFilesWithFile() throws Exception {
        Path file = FileSystemTestUtils.createEmptyFile(fileSystem);

        List<Path> files = finder.findFiles(file);

        assertEquals(files.size(),1);
        assertEquals(files.get(0),file);
    }

    @Test
    public void testGetFilesWithExtension() throws Exception {
        Path file = FileSystemTestUtils.createEmptyFile(fileSystem,"csv");

        List<Path> csvFiles = finder.findFilesWithExtension(file, "csv");
        List<Path> exeFiles = finder.findFilesWithExtension(file, "exe");

        assertEquals(csvFiles.size(),1);
        assertEquals(csvFiles.get(0),file);

        assertEquals(exeFiles.size(),0);
    }

    @Test
    public void testGetFilesWithWildcardExtension() throws Exception {
        Path csvFile = FileSystemTestUtils.createEmptyFile(fileSystem,"csv");

        List<Path> allExtensions = finder.findFilesWithExtension(csvFile, "*");

        assertEquals(allExtensions.size(),1);
        assertEquals(allExtensions.get(0),csvFile);
    }

    @Test
    public void testGetFilesEmptyDir() throws Exception {
        Path emptyDir = FileSystemTestUtils.createEmptyDir(fileSystem);

        List<Path> filesInEmptyDir = finder.findFiles(emptyDir);

        assertTrue(filesInEmptyDir.isEmpty());
    }

    @Test
    public void testGetFilesDirWithFile() throws Exception {
        Path dirWithFiles = FileSystemTestUtils.createDirWithEmptyFile(fileSystem,".any");

        List<Path> filesInEmptyDir = finder.findFiles(dirWithFiles);

        assertTrue(!filesInEmptyDir.isEmpty());
    }

    @Test
    public void testGetFilesInDirWithMultipleExtensions() throws Exception {
        Path dirWithFiles = FileSystemTestUtils.createEmptyDir(fileSystem);

        Path csvFile = FileSystemTestUtils.createEmptyFile(fileSystem,dirWithFiles,"csv");
        FileSystemTestUtils.createEmptyFile(fileSystem,dirWithFiles,"exe");
        FileSystemTestUtils.createEmptyFile(fileSystem,dirWithFiles,"tsv");

        List<Path> csvFiles = finder.findFilesWithExtension(dirWithFiles,"csv");
        assertEquals(csvFiles.size(),1);
        assertEquals(csvFiles.get(0),csvFile);

        List<Path> allFiles = finder.findFiles(dirWithFiles);
        assertEquals(allFiles.size(),3);
    }
}
