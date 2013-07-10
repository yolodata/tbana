package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import org.apache.hadoop.fs.Path;

public class HadoopBucket extends Bucket {

    public HadoopBucket(Path path, Index index, String bucketName) {
        // We don't need the bucketFormat, so set to null.
        super(path.toString(), index.getName(), bucketName, null);
    }
}