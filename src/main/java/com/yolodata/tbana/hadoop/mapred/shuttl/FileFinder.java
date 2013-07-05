package com.yolodata.tbana.hadoop.mapred.shuttl;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileFinder {

    private final FileSystem fileSystem;

    public FileFinder(FileSystem fs) {
        this.fileSystem = fs;
    }

    public List<Path> findFiles(Path path) throws IOException {
        return findFilesWithExtension(path,"*");
    }

    public List<Path> findFilesWithExtension(Path path, String extension) throws IOException {
        FileStatus file = fileSystem.getFileStatus(path);
        List<Path> result = new ArrayList<Path>();

        if(file.isDir())
            for(FileStatus subFile : fileSystem.listStatus(path))
                result.addAll(findFilesWithExtension(subFile.getPath(),extension));
        else
            if(fileHasExtension(path,extension))
                result.add(file.getPath());

        return result;
    }

    private boolean fileHasExtension(Path file, String extension) {
        return extension == "*" || file.toString().endsWith("."+extension);
    }
}
