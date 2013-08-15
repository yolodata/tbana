/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.testutils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class FileSystemTestUtils {

    public static Path createEmptyFile(FileSystem fs) throws IOException {
        return createEmptyFile(fs,".ext");
    }

    public static Path createEmptyFile(FileSystem fs, String extension) throws IOException {
        Path file = new Path(FileTestUtils.getRandomTestFilepath().concat("."+extension));

        assertTrue(fs.createNewFile(file));

        return fs.getFileStatus(file).getPath();
    }

    public static Path createEmptyFile(FileSystem fs, Path location, String extension) throws IOException {

        Path file = HadoopFileTestUtils.createPath(location.toString(), FileTestUtils.getRandomFilename(extension));

        assertTrue(fs.createNewFile(file));

        return fs.getFileStatus(file).getPath();
    }

    public static Path createEmptyDir(FileSystem fileSystem) throws IOException {
        Path dir = new Path(FileTestUtils.getRandomTestFilepath());
        assertTrue(fileSystem.mkdirs(dir));
        return fileSystem.getFileStatus(dir).getPath();
    }

    public static Path createEmptyDir(FileSystem fileSystem, Path directory, String directoryName) throws IOException {
        Path dir = HadoopFileTestUtils.createPath(directory.toString(),directoryName);

        assertTrue(fileSystem.mkdirs(dir));

        return fileSystem.getFileStatus(dir).getPath();
    }


    public static Path createDirWithEmptyFile(FileSystem fileSystem, String extension) throws IOException {
        Path dir = createEmptyDir(fileSystem);

        createEmptyFile(fileSystem, dir, extension);

        return dir;
    }

    public static Path createDirectoryWithEmptyFiles(FileSystem fileSystem, String[] filesToCreate) throws IOException {
        Path directory = FileSystemTestUtils.createEmptyDir(fileSystem);

        for(String filename : filesToCreate) {
            Path filePath = HadoopFileTestUtils.createPath(directory.toString(),filename);
            HadoopFileTestUtils.createFileWithContent(fileSystem,filePath,"");
        }

        return directory;
    }

    public static List<Path> createDirectories(FileSystem fileSystem, Path rootDirectory, String[] directories) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        for(String directoryName : directories) {
            paths.add(createEmptyDir(fileSystem,rootDirectory,directoryName));
        }

        return paths;
    }
}
