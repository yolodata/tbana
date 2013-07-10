package com.yolodata.tbana.util.search.filter;

import com.splunk.shuttl.archiver.model.BucketName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

        String bucketName = (new Path(path)).getName();
        return validateBucketName(bucketName);
    }

    private boolean validateBucketName(String bucketName) {
        BucketName name = new BucketName(bucketName);
        try{
            // this will run the validate name method on the bucket name
            name.getDB();
        } catch(BucketName.IllegalBucketNameException exception)
        {
            return false;
        }

        return true;
    }
}
