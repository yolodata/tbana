package com.yolodata.tbana.util.search.filter;

import com.splunk.shuttl.archiver.model.BucketName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class BucketTimestampFilter extends BucketFilter {

    private final long earliest;
    private final long latest;

    public BucketTimestampFilter(FileSystem fileSystem, long earliest, long latest) {
        super(fileSystem);
        this.earliest = earliest;
        this.latest = latest;
    }

    @Override
    public boolean accept(String path) throws IOException {
        if(!super.accept(path))
            return false;

        BucketName bucket = new BucketName((new Path(path)).getName());

        return bucketWithinTimeRange(bucket);
    }

    private boolean bucketWithinTimeRange(BucketName bucket) {

        if(bucket.getLatest() > earliest)
            return false;
        if (bucket.getEarliest() < latest)
            return false;

        return true;
    }
}
