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
        return accept(new Path(path), directoryFilter);
    }

    public boolean accept(Path path, SearchFilter dependency) throws IOException {
        if(!dependency.accept(path.toString()))
            return false;

        return validateBucketName(path.getName());
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
