package com.yolodata.tbana.util.search.filter;

import com.yolodata.tbana.hadoop.mapred.shuttl.bucket.Bucket;
import org.apache.hadoop.fs.FileSystem;

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

        Bucket bucket = Bucket.create(path);

        return bucketWithinTimeRange(bucket);
    }

    private boolean bucketWithinTimeRange(Bucket bucket) {

        // Improve.me
        return intervalFitsInBucket(bucket) ||
                bucketFitsInInterval(bucket) ||
                partOfBucketFitsInInterval(bucket) ||
                atLeastOneTimeStampIsEqual(bucket);

    }

    private boolean atLeastOneTimeStampIsEqual(Bucket bucket) {
        return bucket.getStart() == earliest || bucket.getStart() == latest ||
                bucket.getEnd() == earliest || bucket.getEnd() == latest;
    }

    private boolean partOfBucketFitsInInterval(Bucket bucket) {
        return (bucket.getStart() > earliest && bucket.getStart() < latest) ||
                (bucket.getEnd() > earliest && bucket.getEnd() < latest);
    }


    private boolean bucketFitsInInterval(Bucket bucket) {
        return (earliest < bucket.getStart() && earliest < bucket.getEnd()) &&
                (latest > bucket.getStart() && latest > bucket.getEnd());
    }

    private boolean intervalFitsInBucket(Bucket bucket) {
        return (earliest > bucket.getStart() && earliest < bucket.getEnd()) &&
                (latest > bucket.getStart() && latest < bucket.getEnd());
    }
}
