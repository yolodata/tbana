package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import org.apache.hadoop.fs.Path;

public class HadoopBucketFactory {

    public static HadoopBucket createUsingPathToBucket(String path, Index index) {
        return createUsingPathToBucket(new Path(path), index);
    }

    public static HadoopBucket createUsingPathToBucket(Path path, Index index) {
        return new HadoopBucket(path,index, path.getName());
    }
}
