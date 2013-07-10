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

        // Improve.me
        return intervalFitsInBucket(bucket) ||
                bucketFitsInInterval(bucket) ||
                partOfBucketFitsInInterval(bucket) ||
                atLeastOneTimeStampIsEqual(bucket);

    }

    private boolean atLeastOneTimeStampIsEqual(BucketName bucket) {
        return bucket.getEarliest() == earliest || bucket.getEarliest() == latest ||
                bucket.getLatest() == earliest || bucket.getLatest() == latest;
    }

    private boolean partOfBucketFitsInInterval(BucketName bucket) {
        return (bucket.getEarliest() < earliest && bucket.getEarliest() > latest) ||
                (bucket.getLatest() < earliest && bucket.getLatest() > latest);
    }


    private boolean bucketFitsInInterval(BucketName bucket) {
        return (earliest > bucket.getEarliest() && earliest > bucket.getLatest()) &&
                (latest < bucket.getEarliest() && latest < bucket.getLatest());
    }

    private boolean intervalFitsInBucket(BucketName bucket) {
        return (earliest < bucket.getEarliest() && earliest > bucket.getLatest()) &&
                (latest < bucket.getEarliest() && latest > bucket.getLatest());
    }
}
