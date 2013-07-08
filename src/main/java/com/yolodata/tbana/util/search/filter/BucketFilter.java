package com.yolodata.tbana.util.search.filter;

import com.yolodata.tbana.hadoop.mapred.shuttl.bucket.DirectoryFilter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class BucketFilter implements SearchFilter {

    private FileSystem fileSystem;

    public BucketFilter(FileSystem fileSystem) {

        this.fileSystem = fileSystem;
    }

    @Override
    public boolean accept(String path) throws IOException {
        DirectoryFilter directoryFilter = new DirectoryFilter(fileSystem);
        if(!directoryFilter.accept(path))
            return false;

        return validateBucketName();

    }

    private boolean validateBucketName() {
        return true;
    }
}
