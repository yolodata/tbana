package com.yolodata.tbana.util.search.filter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ModifiedTimeFilter implements SearchFilter {

    private final FileSystem fileSystem;
    private final int time;

    public ModifiedTimeFilter(FileSystem fileSystem, int time) {
        this.fileSystem = fileSystem;
        this.time = time;
    }

    @Override
    public boolean accept(String path) throws IOException {
        return accept(new Path(path));
    }

    public boolean accept(Path path) throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(path);

        return fileStatus.getModificationTime() > time;
    }
}
