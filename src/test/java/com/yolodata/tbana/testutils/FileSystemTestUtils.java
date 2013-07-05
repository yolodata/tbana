package com.yolodata.tbana.testutils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class FileSystemTestUtils {

    public static Path createEmptyFile(FileSystem fs) throws IOException {
        return createEmptyFile(fs,".ext");
    }

    public static Path createEmptyFile(FileSystem fs, String extension) throws IOException {
        Path file = new Path(TestUtils.getRandomTestFilepath().concat("."+extension));

        assertTrue(fs.createNewFile(file));

        return fs.getFileStatus(file).getPath();
    }

    public static Path createEmptyFile(FileSystem fs, Path location, String extension) throws IOException {

        Path file = TestUtils.createPath(location.toString(),TestUtils.getRandomFilename(extension));

        assertTrue(fs.createNewFile(file));

        return fs.getFileStatus(file).getPath();
    }

    public static Path createEmptyDir(FileSystem fileSystem) throws IOException {
        Path dir = new Path(TestUtils.getRandomTestFilepath());

        assertTrue(fileSystem.mkdirs(dir));

        return fileSystem.getFileStatus(dir).getPath();
    }

    public static Path createDirWithEmptyFile(FileSystem fileSystem, String extension) throws IOException {
        Path dir = createEmptyDir(fileSystem);

        createEmptyFile(fileSystem, dir,extension);

        return dir;
    }
}
