package com.yolodata.tbana.hadoop.mapred.shuttl.bucket;

import com.splunk.shuttl.archiver.model.Bucket;
import com.yolodata.tbana.cascading.splunk.SplunkDataQuery;
import com.yolodata.tbana.hadoop.mapred.shuttl.index.Index;
import org.apache.hadoop.fs.FileSystem;
import com.yolodata.tbana.util.search.HadoopPathFinder;
import com.yolodata.tbana.util.search.PathFinder;
import com.yolodata.tbana.util.search.filter.BucketFilter;
import com.yolodata.tbana.util.search.filter.BucketTimestampFilter;
import com.yolodata.tbana.util.search.filter.SearchFilter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class BucketFinder {

    private FileSystem fileSystem;
    private Index index;

    public BucketFinder(FileSystem fs, Index index) {
        this.fileSystem = fs;
        this.index = index;
    }

    public List<Bucket> search() throws IOException {
        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new BucketFilter(fileSystem));
        return search(filters);
    }

    public List<Bucket> search(SplunkDataQuery splunkSearch) throws IOException {

        long earliest = TimeParser.parse(splunkSearch.getEarliestTime());
        long latest = TimeParser.parse(splunkSearch.getLatestTime());

        List<SearchFilter> filters = new ArrayList<SearchFilter>();
        filters.add(new BucketTimestampFilter(fileSystem,earliest,latest));

        return search(filters);
    }

    public List<Bucket> search(List<SearchFilter> filters) throws IOException {

        List<Bucket> buckets = new ArrayList<Bucket>();

        PathFinder finder = new HadoopPathFinder(fileSystem);
        List<String> bucketPaths = finder.findPaths(index.getPath(), filters);

        for(String bucketPath : bucketPaths) {
            buckets.add(HadoopBucketFactory.createUsingPathToBucket(bucketPath, index));
        }

        return buckets;
    }
}
